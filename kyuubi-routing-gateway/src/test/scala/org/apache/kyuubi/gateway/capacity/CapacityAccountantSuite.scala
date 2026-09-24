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

class CapacityAccountantSuite extends KyuubiFunSuite {

  private val GB = 1024L * 1024 * 1024

  /** 4 workers of 10 GB each = 40 GB now, up to 100 GB if scaled to 10. */
  private val capacity = ClusterCapacity(
    maxMemoryPerNodeBytes = 10 * GB,
    workers = 4,
    maxWorkers = 10)

  private def accountant(policy: AdmissionPolicy.AdmissionPolicy, now: Long = 1000L) =
    new CapacityAccountant(policy, clock = () => now)

  test("workersFor is a ceiling, not a ratio") {
    assert(capacity.workersFor(10 * GB) === 1)
    assert(capacity.workersFor(10 * GB + 1) === 2, "a byte over one node needs a second")
    assert(capacity.workersFor(0) === 0)
  }

  test("admits while memory is left, then asks to scale rather than wait") {
    val a = accountant(AdmissionPolicy.PackByMemory)
    assert(a.admit("c", capacity, "q1", 30 * GB).isRight)
    assert(a.reservedBytes("c") === 30 * GB)

    assert(a.admit("c", capacity, "q2", 10 * GB).isRight, "exactly filling the cluster is allowed")
    // Full at the current 4 workers, but 10 are allowed - q3 is grown into
    // rather than made to wait.
    assert(
      a.admit("c", capacity, "q3", 1).swap.getOrElse(null) === AdmissionDenial.NeedsScaleUp(5))
  }

  test("an idle cluster that is merely too small asks to scale, not to wait") {
    val a = accountant(AdmissionPolicy.PackByMemory)
    // 60 GB needs 6 workers; 4 exist, 10 allowed.
    assert(a.admit("c", capacity, "q1", 60 * GB).swap.getOrElse(null) ===
      AdmissionDenial.NeedsScaleUp(6))
  }

  test("an occupied cluster with room to grow scales instead of waiting") {
    val a = accountant(AdmissionPolicy.PackByMemory)
    a.admit("c", capacity, "q1", 10 * GB)
    // Together q1 and q2 need 7 workers; 10 are allowed, so growing helps
    // even though q1 is still in flight - a bigger cluster does not relieve
    // q1's own memory, but it does make room for q2 alongside it.
    assert(a.admit("c", capacity, "q2", 60 * GB).swap.getOrElse(null) ===
      AdmissionDenial.NeedsScaleUp(7))
  }

  test("an occupied cluster already at its ceiling reports Busy") {
    val a = accountant(AdmissionPolicy.PackByMemory)
    val atCeiling = capacity.copy(workers = capacity.maxWorkers) // fully scaled: 10 workers
    a.admit("c", atCeiling, "q1", 100 * GB) // exactly fills all 10
    // Nothing more fits even at the ceiling; only waiting helps now.
    assert(a.admit("c", atCeiling, "q2", 1).swap.getOrElse(null) === AdmissionDenial.Busy)
  }

  test("beyond the ceiling is refused outright, not queued") {
    val a = accountant(AdmissionPolicy.PackByMemory)
    assert(a.admit("c", capacity, "q1", 200 * GB).swap.getOrElse(null) ===
      AdmissionDenial.TooLarge(20, 10))
  }

  test("Exclusive admits one query however small the next one is") {
    val a = accountant(AdmissionPolicy.Exclusive)
    assert(a.admit("c", capacity, "q1", 1 * GB).isRight)
    assert(a.admit("c", capacity, "q2", 1).swap.getOrElse(null) === AdmissionDenial.Busy)
    a.release("c", "q1")
    assert(a.admit("c", capacity, "q2", 1).isRight)
  }

  test("release frees capacity and is idempotent") {
    val a = accountant(AdmissionPolicy.PackByMemory)
    a.admit("c", capacity, "q1", 40 * GB)
    a.release("c", "q1")
    a.release("c", "q1")
    a.release("c", "never-admitted")
    assert(a.reservedBytes("c") === 0)
    assert(a.admit("c", capacity, "q2", 40 * GB).isRight)
  }

  test("clusters are accounted separately") {
    val a = accountant(AdmissionPolicy.PackByMemory)
    a.admit("c1", capacity, "q1", 40 * GB)
    assert(a.admit("c2", capacity, "q2", 40 * GB).isRight)
    assert(a.reservedBytes("c1") === 40 * GB)
  }

  test("reservations older than the cutoff are reported for reclaim") {
    var now = 10000L
    val a = new CapacityAccountant(AdmissionPolicy.PackByMemory, clock = () => now)
    a.admit("c", capacity, "q1", 1 * GB)

    assert(a.staleReservations(5000L).isEmpty, "a fresh reservation is not stale")

    now = 20000L
    a.admit("c", capacity, "q2", 1 * GB)
    assert(
      a.staleReservations(5000L).map(_._2.queryId) === Seq("q1"),
      "a reservation that outlives any plausible query means its release was missed, " +
        "and left alone it shrinks the cluster for good")
  }

  test("a cluster with no per-node limit known cannot size anything") {
    val unknown = ClusterCapacity(maxMemoryPerNodeBytes = 0, workers = 4, maxWorkers = 10)
    assert(unknown.workersFor(GB) === 0)
    assert(unknown.totalMemoryBytes === 0)
  }
}
