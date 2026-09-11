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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.gateway.capacity.AdmissionPolicy.AdmissionPolicy

/**
 * Decides whether a query fits a cluster right now, and remembers what it let in.
 *
 * The engines cannot answer this themselves. Neither binds a query to a subset
 * of workers - every query spreads over every active node - so "reserve workers
 * for this query" is not expressible inside them. What is expressible is
 * accounting: track what the cluster can hold, subtract what is already
 * admitted, and only let a query in when the remainder covers it.
 *
 * Memory is the quantity tracked because it is the one the engine enforces. CPU
 * is deliberately not: an estimate gives work, not power, so dividing it by
 * capacity yields a duration rather than a requirement.
 *
 * This instance holds its state in memory and is correct for one gateway
 * process. Several gateway replicas admitting against the same cluster would
 * each see only their own reservations and together overcommit it; sharing
 * that state is left to a later change, and the interface is deliberately
 * narrow so it can move without touching callers.
 */
class CapacityAccountant(
    policy: AdmissionPolicy,
    clock: () => Long = () => System.currentTimeMillis) extends Logging {

  // Writes go through `synchronized` because admission is a read-modify-write
  // that must be atomic; the concurrent map is for the read paths - metrics and
  // stale sweeps - which must not block admission while they run.
  private val reservations = new ConcurrentHashMap[String, Map[String, Reservation]]()

  /**
   * Admits a query if the cluster can hold it, and records the reservation.
   *
   * @return Right with the reservation, or Left with why not
   */
  def admit(
      cluster: String,
      capacity: ClusterCapacity,
      queryId: String,
      memoryBytes: Long): Either[AdmissionDenial, Reservation] = synchronized {
    val needed = capacity.workersFor(memoryBytes)
    if (needed > capacity.maxWorkers) {
      return Left(AdmissionDenial.TooLarge(needed, capacity.maxWorkers))
    }

    val held = reservations.getOrDefault(cluster, Map.empty)

    if (policy == AdmissionPolicy.Exclusive && held.nonEmpty) {
      return Left(AdmissionDenial.Busy)
    }

    val reserved = held.values.map(_.memoryBytes).sum
    if (reserved + memoryBytes > capacity.totalMemoryBytes) {
      // Distinguish the two reasons carefully: they lead to different actions.
      // Idle but too small means scaling out helps; occupied means only waiting
      // does, because a bigger cluster still would not free what is in flight.
      if (held.isEmpty && needed > capacity.workers) {
        return Left(AdmissionDenial.NeedsScaleUp(needed))
      }
      return Left(AdmissionDenial.Busy)
    }

    val reservation = Reservation(queryId, memoryBytes, clock())
    reservations.put(cluster, held + (queryId -> reservation))
    debug(s"Admitted $queryId to $cluster reserving $memoryBytes bytes")
    Right(reservation)
  }

  /** Releases a reservation. Unknown ids are ignored - release must be idempotent. */
  def release(cluster: String, queryId: String): Unit = synchronized {
    val held = reservations.getOrDefault(cluster, Map.empty)
    if (held.contains(queryId)) {
      val remaining = held - queryId
      if (remaining.isEmpty) reservations.remove(cluster) else reservations.put(cluster, remaining)
      debug(s"Released $queryId on $cluster")
      // Woken on every release, not only on the one a given waiter needs: this
      // instance does not know how much each waiter wants, and a waiter that
      // wakes to find the room still too small simply waits again.
      notifyAll()
    }
  }

  /** Drops every reservation on a cluster, for when it disappears entirely. */
  def releaseAll(cluster: String): Unit = synchronized {
    reservations.remove(cluster)
    notifyAll()
  }

  /**
   * Waits for capacity to be released, up to `timeoutMillis`.
   *
   * Returns when something was released or the wait ran out - the caller finds
   * out which by trying to admit again, because between waking and retrying
   * another query may have taken the room. This is deliberately not a queue:
   * waiters are not ordered, and a large query can be passed by smaller ones
   * that keep fitting. Fair queueing would need to hold capacity empty while a
   * big query waits, which wastes the cluster for as long as it waits.
   */
  def awaitRelease(timeoutMillis: Long): Unit = synchronized {
    if (timeoutMillis > 0) wait(timeoutMillis)
  }

  def reservedBytes(cluster: String): Long =
    reservations.getOrDefault(cluster, Map.empty).values.map(_.memoryBytes).sum

  def reservationCount(cluster: String): Int =
    reservations.getOrDefault(cluster, Map.empty).size

  /** What is held on a cluster right now, by reservation id. */
  def reservationsOn(cluster: String): Map[String, Reservation] =
    reservations.getOrDefault(cluster, Map.empty)

  /** Clusters with something reserved. Only these are worth reconciling. */
  def clustersWithReservations: Seq[String] = reservations.keySet().asScala.toSeq

  /**
   * Reservations admitted before the given time.
   *
   * A reservation outliving any plausible query means its release was missed -
   * a dropped event, a gateway restart mid-flight. Left alone it shrinks the
   * cluster's apparent capacity for good, so something has to reclaim it.
   */
  def staleReservations(olderThanMillis: Long): Seq[(String, Reservation)] = {
    val cutoff = clock() - olderThanMillis
    reservations.asScala.toSeq.flatMap { case (cluster, held) =>
      held.values.filter(_.admittedAtMillis < cutoff).map(cluster -> _)
    }
  }
}
