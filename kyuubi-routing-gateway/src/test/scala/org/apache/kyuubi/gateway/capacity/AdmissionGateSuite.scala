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
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.gateway.cluster.ScaleTarget
import org.apache.kyuubi.gateway.scaling.ClusterScaler
import org.apache.kyuubi.gateway.sizing.{QueryPlanner, QuerySizer, SizingPolicy}

class AdmissionGateSuite extends KyuubiFunSuite {

  private val GB = 1024L * 1024 * 1024
  private val cluster = ClusterRef("c", "trino", "http://c:8080")
  private val capacity = ClusterCapacity(10 * GB, workers = 4, maxWorkers = 10)

  private def planWith(memoryBytes: Long) =
    s"""{"id":"1","estimates":[{"memoryCost":$memoryBytes.0,"cpuCost":10.0}],"children":[]}"""

  private def gate(policy: AdmissionPolicy.AdmissionPolicy = AdmissionPolicy.PackByMemory) =
    new AdmissionGate(
      new CapacityAccountant(policy),
      new QuerySizer(SizingPolicy(memoryFactor = 1.0)),
      _ => Some(capacity))

  private def planner(plan: => String): QueryPlanner = (_: String) => plan

  test("sizes from the plan and admits what fits") {
    val g = gate()
    val admitted =
      g.admit(cluster, "q1", "SELECT * FROM t", planner(planWith(10 * GB))).toOption.get
    assert(admitted.workers === 1)
    assert(admitted.reservation.map(_.memoryBytes) === Some(10 * GB))
  }

  test("a query too big for the current cluster asks to scale") {
    val g = gate()
    assert(g.admit(cluster, "q1", "SELECT * FROM t", planner(planWith(60 * GB)))
      .swap.getOrElse(null) === AdmissionDenial.NeedsScaleUp(6))
  }

  test("a failing EXPLAIN does not refuse the query") {
    val g = gate()
    val broken = planner(throw new RuntimeException("cannot plan in isolation"))
    assert(
      g.admit(cluster, "q1", "SELECT * FROM t", broken).isRight,
      "the engine, not the gateway, decides whether an unplannable statement runs")
  }

  test("statements that touch no data are not planned") {
    var planned = 0
    val g = gate()
    val counting = planner({ planned += 1; planWith(GB) })
    g.admit(cluster, "q1", "SET SESSION x = 1", counting)
    g.admit(cluster, "q2", "SHOW TABLES", counting)
    assert(planned === 0, "planning a metadata statement costs a round trip and tells us nothing")

    g.admit(cluster, "q3", "SELECT 1", counting)
    assert(planned === 1)
  }

  test("an unrecognised statement is planned rather than assumed free") {
    var planned = 0
    val g = gate()
    val counting = planner({ planned += 1; planWith(GB) })
    g.admit(cluster, "q1", "MERGE INTO t USING s ON t.id = s.id", counting)
    assert(planned === 1, "the safe direction for an unknown statement is to plan it")
  }

  test("EXPLAIN ANALYZE runs its query, so it is planned and sized as that query") {
    var planned = Seq.empty[String]
    val g = gate()
    val recording: QueryPlanner = (statement: String) => { planned :+= statement; planWith(GB) }

    val admitted =
      g.admit(cluster, "q1", "EXPLAIN ANALYZE VERBOSE SELECT * FROM t", recording).toOption.get
    assert(
      planned === Seq("SELECT * FROM t"),
      "the planner is asked about the query, not the EXPLAIN")
    assert(admitted.reservation.isDefined, "it holds memory like the query it runs")
    assert(admitted.workers > 0, "and waits for workers like the query it runs")

    g.admit(cluster, "q2", "EXPLAIN SELECT * FROM t", recording)
    assert(planned.size === 1, "a plain EXPLAIN runs nothing and is not planned")
  }

  test("a cluster with no declared capacity is admitted unaccounted, not refused") {
    val ungated = new AdmissionGate(
      new CapacityAccountant(AdmissionPolicy.PackByMemory),
      new QuerySizer(SizingPolicy(memoryFactor = 1.0)),
      _ => None)
    val admitted = ungated.admit(cluster, "q1", "SELECT 1", planner(planWith(60 * GB))).toOption.get
    assert(admitted.reservation.isEmpty, "refusing would break every cluster not yet annotated")
  }

  test("release returns the capacity to the pool") {
    val g = gate()
    val big = planner(planWith(40 * GB))
    assert(g.admit(cluster, "q1", "SELECT 1", big).isRight)
    assert(g.admit(cluster, "q2", "SELECT 1", big).isLeft)
    g.release("c", "q1")
    assert(g.admit(cluster, "q2", "SELECT 1", big).isRight)
  }

  private val target = ScaleTarget(
    "ns",
    "trino-a",
    "trino.arenadata.io",
    "v1alpha1",
    "clusters")

  /** Grants whatever is asked for, and remembers what that was. */
  private class RecordingScaler(grant: Int => Int = identity) extends ClusterScaler {
    var asked: Option[(ScaleTarget, Int)] = None
    override def ensureAtLeast(t: ScaleTarget, workers: Int): Int = {
      asked = Some((t, workers))
      grant(workers)
    }
  }

  private def scalingGate(scaler: ClusterScaler) =
    new AdmissionGate(
      new CapacityAccountant(AdmissionPolicy.PackByMemory),
      new QuerySizer(SizingPolicy(memoryFactor = 1.0)),
      _ => Some(capacity),
      Some(scaler))

  test("a query too big for the cluster grows it and is admitted for that size") {
    val scaler = new RecordingScaler()
    val scalable = cluster.copy(scaleTarget = Some(target))
    val admitted = scalingGate(scaler)
      .admit(scalable, "q1", "SELECT * FROM t", planner(planWith(60 * GB))).toOption.get

    assert(scaler.asked === Some((target, 6)))
    assert(
      admitted.workers === 6,
      "the query must carry the size it was admitted for, or it starts without those workers")
  }

  test("the query waits for what it needs, not for every worker in the cluster") {
    // The scaler grants more than was asked for - a cluster somebody else just
    // grew, or a minimum the operator enforces.
    val scaler = new RecordingScaler(_ => 9)
    val scalable = cluster.copy(scaleTarget = Some(target))
    val admitted = scalingGate(scaler)
      .admit(scalable, "q1", "SELECT * FROM t", planner(planWith(60 * GB))).toOption.get
    assert(
      admitted.workers === 6,
      "requiring all nine would hold the query until the last one is up, or forever if one is down")
  }

  test("the scale request is capped at the cluster's own ceiling") {
    val scaler = new RecordingScaler()
    val scalable = cluster.copy(scaleTarget = Some(target))
    // 150 GB needs 15 workers; the cluster allows 10, so TooLarge comes first
    // and nothing is asked of the scaler.
    val denial = scalingGate(scaler)
      .admit(scalable, "q1", "SELECT * FROM t", planner(planWith(150 * GB))).swap.getOrElse(null)
    assert(denial === AdmissionDenial.TooLarge(15, 10))
    assert(scaler.asked.isEmpty, "growing past the declared ceiling is not the gateway's to decide")
  }

  test("a scale-up that does not happen refuses rather than admitting on absent workers") {
    val scaler = new RecordingScaler(_ => 0)
    val scalable = cluster.copy(scaleTarget = Some(target))
    val denial = scalingGate(scaler)
      .admit(scalable, "q1", "SELECT * FROM t", planner(planWith(60 * GB))).swap.getOrElse(null)
    assert(denial === AdmissionDenial.NeedsScaleUp(6))
  }

  test("a cluster that declares no scale target is refused, not scaled by guesswork") {
    val scaler = new RecordingScaler()
    val denial = scalingGate(scaler)
      .admit(cluster, "q1", "SELECT * FROM t", planner(planWith(60 * GB))).swap.getOrElse(null)
    assert(denial === AdmissionDenial.NeedsScaleUp(6))
    assert(scaler.asked.isEmpty)
  }

  test("statements that touch no data reserve nothing and never grow the cluster") {
    val scaler = new RecordingScaler()
    val scalable = cluster.copy(scaleTarget = Some(target))
    // One worker and a default size of two: sized as unknown, every SHOW here
    // would be a scale-up.
    val g = new AdmissionGate(
      new CapacityAccountant(AdmissionPolicy.PackByMemory),
      new QuerySizer(SizingPolicy(memoryFactor = 1.0, defaultWorkers = 2)),
      _ => Some(capacity.copy(workers = 1)),
      Some(scaler))

    Seq("SHOW CATALOGS", "EXPLAIN SELECT * FROM t", "SET SESSION x = 1").foreach { statement =>
      val admitted = g.admit(scalable, statement, statement, planner(planWith(GB))).toOption.get
      assert(admitted.reservation.isEmpty, s"$statement holds no memory")
      assert(admitted.workers === 0, s"$statement waits for no workers")
    }
    assert(scaler.asked.isEmpty, "a metadata statement is no reason to grow a cluster")
  }

  test("a busy cluster is grown when there is room, rather than made to wait") {
    val scaler = new RecordingScaler()
    val scalable = cluster.copy(scaleTarget = Some(target))
    val g = scalingGate(scaler)
    assert(g.admit(scalable, "q1", "SELECT 1", planner(planWith(40 * GB))).isRight)
    scaler.asked = None
    val admitted = g.admit(scalable, "q2", "SELECT 1", planner(planWith(40 * GB)))
    assert(
      admitted.isRight,
      "40 GB more fits within the 10-worker ceiling, so it grows rather than refuses")
    assert(
      scaler.asked === Some((target, 8)),
      "q1's 4 workers plus q2's 4 more, within the ceiling of 10")
  }

  test("a busy cluster holds the query until room frees, then admits it") {
    // No headroom to grow into - room can only come from q1 releasing.
    val capped = capacity.copy(maxWorkers = capacity.workers)
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory)
    val g = new AdmissionGate(
      accountant,
      new QuerySizer(SizingPolicy(memoryFactor = 1.0)),
      _ => Some(capped),
      None,
      holdMillis = 30000L)
    val big = planner(planWith(40 * GB))

    assert(g.admit(cluster, "q1", "SELECT 1", big).isRight)

    // q2 does not fit until q1 goes. Release it from another thread while q2
    // is waiting: the point is that the waiter wakes on the release rather
    // than on a timeout, so this finishes in milliseconds, not in 30 seconds.
    val releaser = new Thread(() => {
      Thread.sleep(200)
      g.release("c", "q1")
    })
    releaser.start()

    val startedAt = System.currentTimeMillis()
    val admitted = g.admit(cluster, "q2", "SELECT 1", big)
    val tookMillis = System.currentTimeMillis() - startedAt
    releaser.join()

    assert(admitted.isRight, "the query should have been held, not refused")
    assert(tookMillis < 25000, s"waited ${tookMillis}ms - it woke on the timeout, not the release")
  }

  test("holding gives up and says the cluster is busy") {
    // No headroom to grow into - waiting for a release is the only option.
    val capped = capacity.copy(maxWorkers = capacity.workers)
    val g = new AdmissionGate(
      new CapacityAccountant(AdmissionPolicy.PackByMemory),
      new QuerySizer(SizingPolicy(memoryFactor = 1.0)),
      _ => Some(capped),
      None,
      holdMillis = 150L)
    val big = planner(planWith(40 * GB))
    assert(g.admit(cluster, "q1", "SELECT 1", big).isRight)
    assert(g.admit(cluster, "q2", "SELECT 1", big).swap.getOrElse(null) === AdmissionDenial.Busy)
  }

  test("waiting is not tried on refusals that waiting cannot fix") {
    var waits = 0
    val accountant = new CapacityAccountant(AdmissionPolicy.PackByMemory) {
      override def awaitRelease(timeoutMillis: Long): Unit = {
        waits += 1
        super.awaitRelease(timeoutMillis)
      }
    }
    val g = new AdmissionGate(
      accountant,
      new QuerySizer(SizingPolicy(memoryFactor = 1.0)),
      _ => Some(capacity),
      None,
      holdMillis = 30000L)
    // Needs 15 workers against a ceiling of 10: no amount of waiting helps.
    assert(g.admit(cluster, "q1", "SELECT 1", planner(planWith(150 * GB))).isLeft)
    assert(waits === 0, "holding on a hopeless refusal turns an answer into a timeout")
  }
}
