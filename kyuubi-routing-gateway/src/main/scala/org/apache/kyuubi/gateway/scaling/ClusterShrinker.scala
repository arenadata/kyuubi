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

package org.apache.kyuubi.gateway.scaling

import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.util.control.NonFatal

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.gateway.capacity.{CapacityAccountant, ClusterCapacity, ReservationReconciler}
import org.apache.kyuubi.gateway.cluster.{ClusterRef, ClusterResolver}
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.ThreadUtils

/**
 * Gives back a worker when a cluster has had nothing to do for a while.
 *
 * Growing a cluster is a decision about one query; shrinking one is a decision
 * about the absence of queries, and the gateway is the only party that knows
 * that - it holds the reservations. What it does not own is the pods, so the
 * shrink is a drain followed by a replica count, in that order and never the
 * other way round.
 *
 * Three things make it safe:
 *
 * The worker is named before it is removed. Only a StatefulSet allows that, and
 * a pool that is not one is refused - see [[WorkerPool]].
 *
 * The worker is drained first. Trino announces `DRAINING` to the coordinator,
 * which stops assigning it tasks, and reports `DRAINED` when the ones it had
 * have finished. A worker removed before that takes its query fragments with
 * it.
 *
 * The capacity being removed is reserved for the duration. A query admitted
 * while the drain is in flight would otherwise be sized for a cluster that is
 * about to be smaller; holding the departing worker's memory in the same ledger
 * the gate admits from means such a query is either refused or waits, with no
 * special case anywhere.
 */
class ClusterShrinker(
    accountant: CapacityAccountant,
    resolver: ClusterResolver,
    pool: WorkerPool,
    drain: WorkerDrain,
    scaleApi: ScaleApi,
    capacityOf: ClusterRef => Option[ClusterCapacity],
    idleAfterMillis: Long,
    intervalMillis: Long,
    drainTimeoutMillis: Long,
    clock: () => Long = () => System.currentTimeMillis)
  extends AbstractService("ClusterShrinker") {

  import ClusterShrinker._

  private val poller = ThreadUtils.newDaemonSingleThreadScheduledExecutor("cluster-shrinker")

  /** When each cluster was first seen with nothing reserved on it. */
  private val idleSince = new ConcurrentHashMap[String, Long]()

  override def start(): Unit = {
    poller.scheduleWithFixedDelay(
      () => shrinkAll(),
      intervalMillis,
      intervalMillis,
      TimeUnit.MILLISECONDS)
    super.start()
  }

  override def stop(): Unit = {
    ThreadUtils.shutdown(poller)
    super.stop()
  }

  private[scaling] def shrinkAll(): Unit = resolver.clusters.foreach { cluster =>
    try shrink(cluster)
    catch {
      case NonFatal(e) => warn(s"Could not shrink ${cluster.name}", e)
    }
  }

  private[scaling] def shrink(cluster: ClusterRef): Unit = {
    if (accountant.reservationCount(cluster.name) > 0) {
      // Busy now, so the idle clock starts again when it next falls quiet.
      idleSince.remove(cluster.name)
      return
    }

    val since = idleSince.computeIfAbsent(cluster.name, _ => clock())
    if (clock() - since < idleAfterMillis) return

    val target = cluster.scaleTarget.getOrElse(return)
    val statefulSet = target.workerStatefulSet.getOrElse(return)
    val capacity = capacityOf(cluster).getOrElse {
      // Without a declared capacity there is no figure to hold while draining,
      // and shrinking without holding one races every incoming query.
      debug(s"${cluster.name} declares no capacity, not shrinking it")
      return
    }
    val floor = cluster.capacity.map(_.minWorkers).getOrElse(1)

    val (scheme, port) = addressOf(cluster)
    val departing = pool.departing(target.namespace, statefulSet, scheme, port).getOrElse(return)
    if (departing.replicas <= floor) {
      debug(s"${cluster.name} is at its floor of $floor workers")
      return
    }

    // Reserving through the same ledger the gate admits from is what closes the
    // race: a query arriving now sees the cluster as one worker smaller.
    val reservation = s"$ReservationPrefix${cluster.name}"
    accountant.admit(cluster.name, capacity, reservation, capacity.maxMemoryPerNodeBytes) match {
      case Left(denial) =>
        debug(s"Not shrinking ${cluster.name}: $denial")
      case Right(_) =>
        try drainAndRemove(cluster, departing, target, floor)
        finally accountant.release(cluster.name, reservation)
    }
  }

  private def drainAndRemove(
      cluster: ClusterRef,
      departing: DepartingWorker,
      target: org.apache.kyuubi.gateway.cluster.ScaleTarget,
      floor: Int): Unit = {
    info(s"${cluster.name} has been idle, draining ${departing.url}")
    drain.drain(departing.url)

    if (awaitDrained(departing.url)) {
      scaleApi.request(target, departing.replicas - 1)
      idleSince.remove(cluster.name)
      info(s"${cluster.name} shrunk to ${departing.replicas - 1} workers, floor is $floor")
    } else {
      // Put it back rather than remove it anyway. A worker that did not finish
      // draining still has tasks on it, and the whole point of draining is not
      // to take those with it.
      warn(s"${departing.url} did not reach ${TrinoWorkerDrain.Drained} in " +
        s"${drainTimeoutMillis}ms, returning it to service")
      drain.undrain(departing.url)
    }
  }

  private def awaitDrained(workerUrl: String): Boolean = {
    val deadline = clock() + drainTimeoutMillis
    while (clock() < deadline) {
      drain.state(workerUrl) match {
        case Some(TrinoWorkerDrain.Drained) => return true
        case Some(TrinoWorkerDrain.Active) =>
          // Something put it back - another gateway replica, or an operator.
          // Its decision stands.
          warn(s"$workerUrl returned to ${TrinoWorkerDrain.Active} while draining")
          return false
        case _ =>
      }
      Thread.sleep(math.min(DrainPollMillis, math.max(1L, deadline - clock())))
    }
    false
  }

  /**
   * Workers are reached on the scheme and port the coordinator is, because they
   * run the same image with the same configuration. An address that is wrong
   * shows as a worker that never drains, which the timeout turns back.
   */
  private def addressOf(cluster: ClusterRef): (String, Int) = {
    val uri = URI.create(cluster.url)
    val port = if (uri.getPort > 0) uri.getPort else DefaultPort
    (Option(uri.getScheme).getOrElse("http"), port)
  }
}

object ClusterShrinker {

  val ENABLED_KEY = "kyuubi.gateway.scaling.shrink.enabled"
  val IDLE_AFTER_KEY = "kyuubi.gateway.scaling.shrink.idleAfter"
  val INTERVAL_KEY = "kyuubi.gateway.scaling.shrink.interval"
  val DRAIN_TIMEOUT_KEY = "kyuubi.gateway.scaling.shrink.drainTimeout"
  val USER_KEY = "kyuubi.gateway.scaling.shrink.user"

  val DefaultIdleAfterMillis = 600000L
  val DefaultIntervalMillis = 60000L

  /**
   * How long a worker is given to finish what it has.
   *
   * Generous on purpose: the alternative to waiting is either failing the
   * queries on that worker or leaving it drained and idle, and five minutes of
   * one worker costs less than either.
   */
  val DefaultDrainTimeoutMillis = 300000L

  private val DrainPollMillis = 2000L
  private val DefaultPort = 8080

  /** Marks the ledger entry a drain holds, so it is not mistaken for a query. */
  val ReservationPrefix = "shrink:"

  def idleAfterFrom(conf: KyuubiConf): Long =
    conf.getOption(IDLE_AFTER_KEY).map(_.toLong).getOrElse(DefaultIdleAfterMillis)

  def intervalFrom(conf: KyuubiConf): Long =
    conf.getOption(INTERVAL_KEY).map(_.toLong).getOrElse(DefaultIntervalMillis)

  def drainTimeoutFrom(conf: KyuubiConf): Long =
    conf.getOption(DRAIN_TIMEOUT_KEY).map(_.toLong).getOrElse(DefaultDrainTimeoutMillis)

  /**
   * The Trino user a drain speaks as.
   *
   * Trino refuses `PUT /v1/info/state` without an identity even when no
   * authentication is configured - its insecure authenticator still wants
   * `X-Trino-User` - so the drain has to be someone. The reconciler already
   * carries a Trino user for the same reason; reuse it unless told otherwise,
   * and fall back to the OS user as it does.
   */
  def userFrom(conf: KyuubiConf): String =
    conf.getOption(USER_KEY)
      .orElse(conf.getOption(ReservationReconciler.USER_KEY))
      .getOrElse(org.apache.kyuubi.Utils.currentUser)
}
