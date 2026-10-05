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

/**
 * Where reservations live.
 *
 * Admission is a read-modify-write - look at what is held, decide, record the
 * decision - and it is only correct if that whole sequence is atomic against
 * every other admitter. This interface exists so the decision stays in one
 * place while the atomicity can be provided by a mutex within one process or by
 * a compare-and-swap across several.
 */
trait ReservationStore {

  /**
   * Applies `decide` to a cluster's reservations atomically.
   *
   * The function is given what is held and returns what should be held together
   * with the answer to hand back. It may be called more than once when the
   * store has to retry, so it must not do anything but decide.
   */
  def update[A](cluster: String)(
      decide: Map[String, Reservation] => (Map[String, Reservation], A)): A

  def read(cluster: String): Map[String, Reservation]

  /** Clusters with something reserved. */
  def clusters: Seq[String]

  /**
   * Waits for a release, up to `timeoutMillis`.
   *
   * Returning early is always allowed - the caller retries and finds out for
   * itself whether there is room - so a store that cannot observe releases may
   * simply sleep.
   */
  def awaitRelease(timeoutMillis: Long): Unit

  /** Called after a change, so waiters can be woken where that is possible. */
  def signalRelease(): Unit
}

/**
 * Reservations held in this process.
 *
 * Correct for one gateway. Several replicas admitting against one cluster would
 * each see only their own reservations and together overcommit it.
 */
class InMemoryReservationStore extends ReservationStore {

  private val held = new ConcurrentHashMap[String, Map[String, Reservation]]()

  override def update[A](cluster: String)(
      decide: Map[String, Reservation] => (Map[String, Reservation], A)): A = synchronized {
    val (next, answer) = decide(held.getOrDefault(cluster, Map.empty))
    if (next.isEmpty) held.remove(cluster) else held.put(cluster, next)
    answer
  }

  override def read(cluster: String): Map[String, Reservation] =
    held.getOrDefault(cluster, Map.empty)

  override def clusters: Seq[String] = held.keySet().asScala.toSeq

  override def awaitRelease(timeoutMillis: Long): Unit = synchronized {
    if (timeoutMillis > 0) wait(timeoutMillis)
  }

  override def signalRelease(): Unit = synchronized {
    notifyAll()
  }
}
