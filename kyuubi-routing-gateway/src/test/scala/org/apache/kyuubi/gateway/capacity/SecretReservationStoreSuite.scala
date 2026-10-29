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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.kyuubi.KyuubiFunSuite

class SecretReservationStoreSuite extends KyuubiFunSuite {

  private val GB = 1024L * 1024 * 1024
  // No headroom beyond what exists: a losing write should report Busy, not
  // ask to scale into room this cluster does not have.
  private val capacity = ClusterCapacity(10 * GB, workers = 4, maxWorkers = 4)

  private var api: FakeApiServer = _
  private var client: KubernetesClient = _

  override def beforeEach(): Unit = {
    api = new FakeApiServer
    client = api.client
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    if (client != null) client.close()
    if (api != null) api.stop()
    super.afterEach()
  }

  private def store = new SecretReservationStore(client, "gw")

  test("a reservation survives a round trip through the api server") {
    val s = store
    s.update("ns/a")(held => (held + ("r1" -> Reservation("r1", 5 * GB, 1000L)), ()))

    assert(s.read("ns/a") === Map("r1" -> Reservation("r1", 5 * GB, 1000L)))
    assert(s.clusters === Seq("ns/a"))
  }

  test("a losing write is retried against the new state, not replayed") {
    val s = store
    // Another replica reserves 30 GB between this store's read and its write -
    // the interleaving that loses a reservation when writes are unconditional.
    val wedged = new AtomicInteger(0)
    api.beforeWrite = () => {
      if (wedged.getAndIncrement() == 0) {
        new SecretReservationStore(client, "gw")
          .update("ns/a")(held => (held + ("other" -> Reservation("other", 30 * GB, 1L)), ()))
      }
    }

    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory, s)
    val result = accountant.admit("ns/a", capacity, "mine", 30 * GB)

    assert(api.conflicts.get() > 0, "the losing write must have been rejected, not merged")
    assert(
      result.swap.getOrElse(null) === AdmissionDenial.Busy,
      "the retry must decide again against what is now there, not re-apply the old decision")
    assert(s.read("ns/a").keySet === Set("other"))
  }

  test("releasing the last reservation removes the ledger") {
    val s = store
    s.update("ns/a")(held => (held + ("r1" -> Reservation("r1", GB, 1L)), ()))
    assert(s.clusters === Seq("ns/a"))

    s.update("ns/a")(held => (held - "r1", ()))
    assert(s.read("ns/a").isEmpty)
    assert(s.clusters.isEmpty, "an empty ledger left behind would accumulate one per cluster")
  }

  test("an unreadable ledger is treated as empty rather than blocking the cluster") {
    val s = store
    api.put(s.nameFor("ns/a"), "this is not json")
    assert(
      s.read("ns/a").isEmpty,
      "a cluster that can never be admitted to again is worse than a lost ledger")
  }

  test("cluster names become valid Secret names without colliding") {
    val s = store
    val a = s.nameFor("team-a/trino-prod")
    val b = s.nameFor("team_a/trino-prod")
    assert(a.matches("[a-z0-9]([-a-z0-9]*[a-z0-9])?"), s"$a is not a DNS subdomain label")
    assert(a.length <= 63)
    assert(a.contains("team-a-trino-prod"), s"$a should still be readable")
    assert(a !== b, "two clusters must not sanitise onto one ledger")
  }

  test("concurrent admissions from many replicas never exceed the capacity") {
    val threads = 8
    val ready = new CountDownLatch(threads)
    val go = new CountDownLatch(1)
    val admitted = new AtomicInteger(0)

    // Each thread has its own store, as separate gateway processes would.
    val workers = (1 to threads).map { i =>
      val thread = new Thread(() => {
        val accountant = new CapacityAccountant(
          AdmissionPolicy.PackByMemory,
          new SecretReservationStore(client, "gw", maxAttempts = 40))
        ready.countDown()
        go.await()
        if (accountant.admit("ns/a", capacity, s"q$i", 10 * GB).isRight) admitted.incrementAndGet()
      })
      thread.start()
      thread
    }

    ready.await(10, TimeUnit.SECONDS)
    go.countDown()
    workers.foreach(_.join(30000))

    // 40 GB of capacity, 10 GB a query: four fit and no more. Without the
    // compare-and-swap all eight would be admitted, each replica seeing an
    // empty ledger.
    assert(api.conflicts.get() > 0, "no write was ever rejected - the ledger was not contended")
    assert(admitted.get() === 4, s"admitted ${admitted.get()} queries into a cluster holding 4")
    assert(store.read("ns/a").size === 4)
  }
}
