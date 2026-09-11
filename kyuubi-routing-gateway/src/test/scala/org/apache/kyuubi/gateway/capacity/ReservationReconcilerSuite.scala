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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.gateway.cluster.{ClusterRef, ClusterResolver}

class ReservationReconcilerSuite extends KyuubiFunSuite {

  private val GB = 1024L * 1024 * 1024
  private val cluster = ClusterRef("c", "trino", "http://c:8080")
  private val capacity = ClusterCapacity(10 * GB, workers = 8, maxWorkers = 8)

  private class Clusters(refs: Seq[ClusterRef]) extends ClusterResolver {
    override def clusters: Seq[ClusterRef] = refs
    override def resolve(user: String, sessionConf: Map[String, String]): Option[ClusterRef] =
      refs.headOption
  }

  private class Fixed(answer: Option[Seq[ObservedQuery]]) extends ClusterQueries {
    var asked = 0
    override def observe(c: ClusterRef): Option[Seq[ObservedQuery]] = {
      asked += 1
      answer
    }
  }

  /** Admits two queries and returns the accountant plus a movable clock. */
  private def fixture = {
    var now = 1000000L
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory, () => now)
    accountant.admit("c", capacity, "r1", 10 * GB)
    accountant.admit("c", capacity, "r2", 10 * GB)
    (accountant, () => now, (t: Long) => now = t)
  }

  private def reconciler(
      accountant: CapacityAccountant,
      queries: ClusterQueries,
      clock: () => Long,
      calibration: Option[MemoryCalibration] = None) =
    new ReservationReconciler(
      accountant,
      new Clusters(Seq(cluster)),
      queries,
      calibration,
      graceMillis = 60000L,
      clock = clock)

  test("a reservation the cluster is not running is released") {
    val (accountant, clock, advance) = fixture
    // r1 still running, r2 gone from the coordinator entirely.
    val queries = new Fixed(Some(Seq(ObservedQuery("r1", finished = false, None))))
    advance(clock() + 120000L)

    reconciler(accountant, queries, clock).reconcile(cluster)

    assert(accountant.reservationsOn("c").keySet === Set("r1"))
    assert(accountant.reservedBytes("c") === 10 * GB)
  }

  test("a reservation younger than the grace period is left alone") {
    val (accountant, clock, _) = fixture
    val queries = new Fixed(Some(Seq.empty))

    reconciler(accountant, queries, clock).reconcile(cluster)

    assert(
      accountant.reservationsOn("c").keySet === Set("r1", "r2"),
      "a query admitted moments ago has not reached the coordinator yet")
  }

  test("an unreachable coordinator releases nothing") {
    val (accountant, clock, advance) = fixture
    val queries = new Fixed(None)
    advance(clock() + 600000L)

    reconciler(accountant, queries, clock).reconcile(cluster)

    assert(
      accountant.reservationsOn("c").keySet === Set("r1", "r2"),
      "unreachable is not idle - over-admitting onto a struggling cluster is worse")
  }

  test("a finished query is released and its cost recorded") {
    val (accountant, clock, advance) = fixture
    val calibration = new MemoryCalibration
    val queries = new Fixed(Some(Seq(
      ObservedQuery("r1", finished = true, Some(5 * GB)),
      ObservedQuery("r2", finished = false, None))))
    advance(clock() + 120000L)

    reconciler(accountant, queries, clock, Some(calibration)).reconcile(cluster)

    assert(accountant.reservationsOn("c").keySet === Set("r2"))
    assert(calibration.samples("c") === 1)
    // Reserved 10 GB for a query that used 5: the sizing was twice what it needed.
    assert(calibration.observedFactor("c") === Some(2.0))
  }

  test("a cluster the resolver no longer knows has its reservations dropped") {
    val (accountant, clock, _) = fixture
    val queries = new Fixed(Some(Seq.empty))
    val orphaned = new ReservationReconciler(
      accountant,
      new Clusters(Seq.empty),
      queries,
      None,
      graceMillis = 60000L,
      clock = clock)

    orphaned.reconcileAll()

    assert(accountant.reservationsOn("c").isEmpty)
    assert(queries.asked === 0, "a cluster that is gone cannot be asked")
  }

  test("clusters with nothing reserved are not polled") {
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory)
    val queries = new Fixed(Some(Seq.empty))
    new ReservationReconciler(
      accountant,
      new Clusters(Seq(cluster)),
      queries,
      None).reconcileAll()
    assert(queries.asked === 0)
  }
}
