/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.gateway.capacity

import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.gateway.cluster.{ClusterRef, ClusterResolver}
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.ThreadUtils

/**
 * Reclaims reservations whose queries are over, and reports what they cost.
 *
 * Releasing on the client's behalf is the normal path and covers the normal
 * case. It cannot cover a client that disappears mid-query or a gateway
 * restarted while queries were in flight, and a reservation left behind by
 * either shrinks the cluster's apparent capacity permanently - the gateway
 * would refuse queries to protect memory nothing is using.
 *
 * The cluster is asked rather than a timer trusted, because age alone cannot
 * distinguish a long query from a lost one, and picking a cutoff means choosing
 * which of the two to get wrong.
 */
class ReservationReconciler(
    accountant: CapacityAccountant,
    resolver: ClusterResolver,
    queries: ClusterQueries,
    calibration: Option[MemoryCalibration] = None,
    graceMillis: Long = ReservationReconciler.DefaultGraceMillis,
    intervalMillis: Long = ReservationReconciler.DefaultIntervalMillis,
    clock: () => Long = () => System.currentTimeMillis)
  extends AbstractService("ReservationReconciler") {

  private val poller = ThreadUtils.newDaemonSingleThreadScheduledExecutor("reservation-reconciler")

  override def start(): Unit = {
    poller.scheduleWithFixedDelay(
      () => reconcileAll(),
      intervalMillis,
      intervalMillis,
      TimeUnit.MILLISECONDS)
    super.start()
  }

  override def stop(): Unit = {
    ThreadUtils.shutdown(poller)
    super.stop()
  }

  private[capacity] def reconcileAll(): Unit = {
    val byName = resolver.clusters.map(c => c.name -> c).toMap
    accountant.clustersWithReservations.foreach { name =>
      try {
        byName.get(name) match {
          case Some(cluster) => reconcile(cluster)
          case None =>
            // The cluster is gone from the resolver's view. Its reservations
            // cannot be checked and can never be released by a client, so they
            // would sit there forever accounting for a cluster that no longer
            // exists.
            info(s"Cluster $name is no longer known, dropping its reservations")
            accountant.releaseAll(name)
        }
      } catch {
        case NonFatal(e) => warn(s"Failed to reconcile reservations on $name", e)
      }
    }
  }

  private[capacity] def reconcile(cluster: ClusterRef): Unit = {
    val held = accountant.reservationsOn(cluster.name)
    if (held.isEmpty) return

    queries.observe(cluster) match {
      case None =>
        // Unreachable is not the same as idle. Holding the reservations keeps
        // the gateway conservative while the coordinator is unreachable, which
        // is the right way to be wrong: over-admitting onto a cluster that is
        // already struggling is worse than refusing for a while.
        debug(s"Could not reconcile ${cluster.name}, keeping ${held.size} reservations")
      case Some(observed) =>
        val live = observed.filterNot(_.finished).map(_.reservationId).toSet
        val cutoff = clock() - graceMillis

        observed.filter(_.finished).foreach { query =>
          held.get(query.reservationId).foreach { reservation =>
            calibration.foreach(_.record(cluster.name, reservation, query.peakMemoryBytes))
          }
        }

        held.foreach { case (id, reservation) =>
          // Only reservations old enough to have shown up are judged: a query
          // admitted a moment ago has not reached the coordinator yet, and
          // releasing it would hand its memory to somebody else.
          if (!live.contains(id) && reservation.admittedAtMillis < cutoff) {
            info(s"Releasing $id on ${cluster.name}: the cluster is not running it")
            accountant.release(cluster.name, id)
          }
        }
    }
  }
}

object ReservationReconciler {

  val ENABLED_KEY = "kyuubi.gateway.reconcile.enabled"
  val INTERVAL_KEY = "kyuubi.gateway.reconcile.interval"
  val GRACE_KEY = "kyuubi.gateway.reconcile.grace"
  val USER_KEY = "kyuubi.gateway.reconcile.user"

  val DefaultIntervalMillis = 30000L

  /**
   * How long a reservation is left alone before the cluster's silence about it
   * counts as evidence.
   *
   * This is the window between admitting a query and the coordinator listing
   * it: the statement still has to be sent, and with a worker requirement it
   * may wait before it is scheduled. Too short releases capacity out from under
   * a query that is about to start.
   */
  val DefaultGraceMillis = 60000L

  def intervalFrom(conf: KyuubiConf): Long =
    conf.getOption(INTERVAL_KEY).map(_.toLong).getOrElse(DefaultIntervalMillis)

  def graceFrom(conf: KyuubiConf): Long =
    conf.getOption(GRACE_KEY).map(_.toLong).getOrElse(DefaultGraceMillis)
}
