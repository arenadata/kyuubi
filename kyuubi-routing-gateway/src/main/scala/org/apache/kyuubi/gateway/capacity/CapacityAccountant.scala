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
 * Where the reservations live is the store's business. The decision below is
 * the same either way, and is run inside whatever atomicity the store provides,
 * so it must stay a decision and do nothing else.
 */
class CapacityAccountant(
    policy: AdmissionPolicy,
    store: ReservationStore = new InMemoryReservationStore,
    clock: () => Long = () => System.currentTimeMillis) extends Logging {

  /**
   * Admits a query if the cluster can hold it, and records the reservation.
   *
   * @return Right with the reservation, or Left with why not
   */
  def admit(
      cluster: String,
      capacity: ClusterCapacity,
      queryId: String,
      memoryBytes: Long): Either[AdmissionDenial, Reservation] = {
    val needed = capacity.workersFor(memoryBytes)
    if (needed > capacity.maxWorkers) {
      return Left(AdmissionDenial.TooLarge(needed, capacity.maxWorkers))
    }

    store.update(cluster) { held =>
      if (policy == AdmissionPolicy.Exclusive && held.nonEmpty) {
        (held, Left(AdmissionDenial.Busy))
      } else {
        val reserved = held.values.map(_.memoryBytes).sum
        if (reserved + memoryBytes > capacity.totalMemoryBytes) {
          // Distinguish the two reasons carefully: they lead to different
          // actions. Idle but too small means scaling out helps; occupied means
          // only waiting does, because a bigger cluster still would not free
          // what is in flight.
          if (held.isEmpty && needed > capacity.workers) {
            (held, Left(AdmissionDenial.NeedsScaleUp(needed)))
          } else {
            (held, Left(AdmissionDenial.Busy))
          }
        } else {
          val reservation = Reservation(queryId, memoryBytes, clock())
          (held + (queryId -> reservation), Right(reservation))
        }
      }
    }
  }

  /** Releases a reservation. Unknown ids are ignored - release must be idempotent. */
  def release(cluster: String, queryId: String): Unit = {
    val released = store.update(cluster) { held =>
      if (held.contains(queryId)) (held - queryId, true) else (held, false)
    }
    if (released) {
      debug(s"Released $queryId on $cluster")
      // Woken on every release, not only on the one a given waiter needs: the
      // accountant does not know how much each waiter wants, and a waiter that
      // wakes to find the room still too small simply waits again.
      store.signalRelease()
    }
  }

  /** Drops every reservation on a cluster, for when it disappears entirely. */
  def releaseAll(cluster: String): Unit = {
    store.update(cluster)(_ => (Map.empty, ()))
    store.signalRelease()
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
  def awaitRelease(timeoutMillis: Long): Unit = store.awaitRelease(timeoutMillis)

  def reservedBytes(cluster: String): Long =
    store.read(cluster).values.map(_.memoryBytes).sum

  def reservationCount(cluster: String): Int = store.read(cluster).size

  /** What is held on a cluster right now, by reservation id. */
  def reservationsOn(cluster: String): Map[String, Reservation] = store.read(cluster)

  /** Clusters with something reserved. Only these are worth reconciling. */
  def clustersWithReservations: Seq[String] = store.clusters

  /**
   * Reservations admitted before the given time.
   *
   * A reservation outliving any plausible query means its release was missed -
   * a dropped event, a gateway restart mid-flight. Left alone it shrinks the
   * cluster's apparent capacity for good, so something has to reclaim it.
   */
  def staleReservations(olderThanMillis: Long): Seq[(String, Reservation)] = {
    val cutoff = clock() - olderThanMillis
    store.clusters.flatMap { cluster =>
      store.read(cluster).values.filter(_.admittedAtMillis < cutoff).map(cluster -> _)
    }
  }
}
