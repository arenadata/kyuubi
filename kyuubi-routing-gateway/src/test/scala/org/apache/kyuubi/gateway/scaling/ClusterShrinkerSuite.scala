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

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.gateway.capacity.{AdmissionPolicy, CapacityAccountant, ClusterCapacity}
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.gateway.cluster.ClusterResolver
import org.apache.kyuubi.gateway.cluster.DeclaredCapacity
import org.apache.kyuubi.gateway.cluster.ScaleTarget

class ClusterShrinkerSuite extends KyuubiFunSuite {

  private val GB = 1024L * 1024 * 1024
  private val capacity = ClusterCapacity(10 * GB, workers = 4, maxWorkers = 10)

  private val target = ScaleTarget(
    "ns",
    "trino-a",
    "trino.arenadata.io",
    "v1alpha1",
    "clusters",
    workerStatefulSet = Some("trino-a-worker"))

  private def cluster(minWorkers: Int = 1, statefulSet: Boolean = true) = ClusterRef(
    "ns/a",
    "trino",
    "http://trino-a.ns:8080",
    capacity = Some(DeclaredCapacity(10 * GB, 4, 10, minWorkers)),
    scaleTarget = Some(
      if (statefulSet) target else target.copy(workerStatefulSet = None)))

  private class Clusters(refs: Seq[ClusterRef]) extends ClusterResolver {
    override def clusters: Seq[ClusterRef] = refs
    override def resolve(u: String, c: Map[String, String]): Option[ClusterRef] = refs.headOption
  }

  private class FakePool(replicas: Int, statefulSet: Boolean = true) extends WorkerPool {
    var asked = 0
    override def departing(ns: String, sts: String, scheme: String, port: Int)
        : Option[DepartingWorker] = {
      asked += 1
      if (!statefulSet) None
      else Some(DepartingWorker(s"$scheme://$sts-${replicas - 1}.$sts.$ns:$port", replicas))
    }
  }

  private class FakeDrain(
      reaches: String = TrinoWorkerDrain.Drained,
      startState: String = TrinoWorkerDrain.Active) extends WorkerDrain {
    val events = ArrayBuffer.empty[String]
    private var currentState: String = startState
    override def drain(cluster: ClusterRef, url: String): Unit = {
      events += s"drain $url"
      currentState = reaches
    }
    override def undrain(cluster: ClusterRef, url: String): Unit = {
      events += s"undrain $url"
      currentState = TrinoWorkerDrain.Active
    }
    override def state(cluster: ClusterRef, url: String): Option[String] = Some(currentState)
  }

  private class FakeScale extends ScaleApi {
    var requested: Option[Int] = None
    override def ready(t: ScaleTarget): Option[Int] = None
    override def request(t: ScaleTarget, workers: Int): Unit = requested = Some(workers)
  }

  private def shrinker(
      accountant: CapacityAccountant,
      pool: WorkerPool,
      drain: WorkerDrain,
      scale: ScaleApi,
      refs: Seq[ClusterRef],
      now: () => Long,
      idleAfter: Long = 1000L) =
    new ClusterShrinker(
      accountant,
      new Clusters(refs),
      pool,
      drain,
      scale,
      _ => Some(capacity),
      idleAfterMillis = idleAfter,
      intervalMillis = 1000L,
      drainTimeoutMillis = 5000L,
      clock = now)

  test("an idle cluster gives back its highest-ordinal worker, drained first") {
    var now = 1000L
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory, clock = () => now)
    val pool = new FakePool(4)
    val drain = new FakeDrain()
    val scale = new FakeScale()
    val s = shrinker(accountant, pool, drain, scale, Seq(cluster()), () => now)

    s.shrinkAll() // starts the idle clock
    assert(scale.requested.isEmpty, "not idle long enough yet")

    now += 2000
    s.shrinkAll()

    assert(drain.events.head.startsWith("drain "), "the worker must be drained before it goes")
    assert(
      drain.events.head.contains("trino-a-worker-3"),
      s"the departing worker is the highest ordinal, got ${drain.events.head}")
    assert(scale.requested === Some(3))
    assert(!drain.events.contains("undrain"), "a drained worker is not put back")
  }

  test("a busy cluster is left alone and its idle clock restarts") {
    var now = 1000L
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory, clock = () => now)
    val pool = new FakePool(4)
    val scale = new FakeScale()
    val s = shrinker(accountant, pool, new FakeDrain(), scale, Seq(cluster()), () => now)

    s.shrinkAll()
    now += 2000
    accountant.admit("ns/a", capacity, "q1", GB)
    s.shrinkAll()
    assert(scale.requested.isEmpty)

    // The query ends, and the wait starts again rather than resuming.
    accountant.release("ns/a", "q1")
    s.shrinkAll()
    assert(scale.requested.isEmpty, "idle time must be counted from when it fell quiet")

    now += 2000
    s.shrinkAll()
    assert(scale.requested === Some(3))
  }

  test("a worker that will not drain is returned to service, not removed") {
    var now = 1000L
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory, clock = () => now)
    val drain = new FakeDrain(reaches = TrinoWorkerDrain.Draining)
    val scale = new FakeScale()
    // The clock is what the drain wait uses, so it runs out on its own.
    val s = shrinker(
      accountant,
      new FakePool(4),
      drain,
      scale,
      Seq(cluster()),
      () => {
        now += 1000; now
      })

    s.shrinkAll()
    s.shrinkAll()

    assert(scale.requested.isEmpty, "a worker still holding tasks must not be removed")
    assert(drain.events.exists(_.startsWith("undrain ")))
  }

  test("a worker left drained by an interrupted earlier attempt is picked up, not re-drained") {
    var now = 1000L
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory, clock = () => now)
    // A previous attempt reached Drained and then the process died before it
    // could shrink the replica count - the worker is left over, still
    // Drained, and Trino refuses a second transition to Draining from there.
    val drain = new FakeDrain(startState = TrinoWorkerDrain.Drained) {
      override def drain(cluster: ClusterRef, url: String): Unit =
        throw new IllegalStateException(s"$url refused the transition to DRAINING: HTTP 400")
    }
    val scale = new FakeScale()
    val s = shrinker(accountant, new FakePool(4), drain, scale, Seq(cluster()), () => now)

    s.shrinkAll() // starts the idle clock
    now += 2000
    s.shrinkAll()

    assert(scale.requested === Some(3), "the leftover drained worker is still removed")
  }

  test("a pool that is not a StatefulSet is refused rather than shrunk on a guess") {
    var now = 1000L
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory, clock = () => now)
    val drain = new FakeDrain()
    val scale = new FakeScale()
    val s = shrinker(
      accountant,
      new FakePool(4, statefulSet = false),
      drain,
      scale,
      Seq(cluster(statefulSet = false)),
      () => now)

    s.shrinkAll()
    now += 2000
    s.shrinkAll()

    assert(drain.events.isEmpty, "draining one pod while Kubernetes removes another kills queries")
    assert(scale.requested.isEmpty)
  }

  test("a cluster at its floor is not shrunk further") {
    var now = 1000L
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory, clock = () => now)
    val drain = new FakeDrain()
    val scale = new FakeScale()
    val s = shrinker(
      accountant,
      new FakePool(2),
      drain,
      scale,
      Seq(cluster(minWorkers = 2)),
      () => now)

    s.shrinkAll()
    now += 2000
    s.shrinkAll()

    assert(scale.requested.isEmpty)
    assert(drain.events.isEmpty)
  }

  test("the departing worker's capacity is held while it drains") {
    var now = 1000L
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory, clock = () => now)
    val held = ArrayBuffer.empty[Long]
    val drain = new FakeDrain() {
      override def state(cluster: ClusterRef, url: String): Option[String] = {
        // Observed from inside the drain: what a query arriving now would see.
        held += accountant.reservedBytes("ns/a")
        Some(TrinoWorkerDrain.Drained)
      }
    }
    val s = shrinker(accountant, new FakePool(4), drain, new FakeScale(), Seq(cluster()), () => now)

    s.shrinkAll()
    now += 2000
    s.shrinkAll()

    assert(
      held.contains(10 * GB),
      s"one worker's memory must be reserved for the drain, saw $held")
    assert(
      accountant.reservedBytes("ns/a") === 0,
      "and released afterwards, or the cluster shrinks by one worker every pass")
  }
}
