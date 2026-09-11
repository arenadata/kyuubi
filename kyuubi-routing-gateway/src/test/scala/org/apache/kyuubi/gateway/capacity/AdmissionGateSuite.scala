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
import org.apache.kyuubi.gateway.sizing.{QuerySizer, SizingPolicy}

class AdmissionGateSuite extends KyuubiFunSuite {

  private val GB = 1024L * 1024 * 1024
  private val cluster = ClusterRef("c", "trino", "http://c:8080")
  private val capacity = ClusterCapacity(10 * GB, workers = 4, maxWorkers = 10)

  private def planWith(memoryBytes: Long) =
    s"""{"id":"1","estimates":[{"memoryCost":$memoryBytes.0,"cpuCost":10.0}],"children":[]}"""

  private def gate(
      policy: AdmissionPolicy.AdmissionPolicy = AdmissionPolicy.PackByMemory,
      explain: (ClusterRef, String) => String = (_, _) => planWith(10 * GB)) =
    new AdmissionGate(
      new CapacityAccountant(policy),
      new QuerySizer(SizingPolicy(memoryFactor = 1.0)),
      _ => Some(capacity),
      explain)

  test("sizes from the plan and admits what fits") {
    val g = gate()
    val admitted = g.admit(cluster, "q1", "SELECT * FROM t").toOption.get
    assert(admitted.workers === 1)
    assert(admitted.reservation.map(_.memoryBytes) === Some(10 * GB))
  }

  test("a query too big for the current cluster asks to scale") {
    val g = gate(explain = (_, _) => planWith(60 * GB))
    assert(g.admit(cluster, "q1", "SELECT * FROM t").swap.getOrElse(null) ===
      AdmissionDenial.NeedsScaleUp(6))
  }

  test("a failing EXPLAIN does not refuse the query") {
    val g = gate(explain = (_, _) => throw new RuntimeException("cannot plan in isolation"))
    assert(g.admit(cluster, "q1", "SELECT * FROM t").isRight,
      "the engine, not the gateway, decides whether an unplannable statement runs")
  }

  test("statements that touch no data are not planned") {
    var planned = 0
    val g = gate(explain = (_, _) => { planned += 1; planWith(GB) })
    g.admit(cluster, "q1", "SET SESSION x = 1")
    g.admit(cluster, "q2", "SHOW TABLES")
    assert(planned === 0, "planning a metadata statement costs a round trip and tells us nothing")

    g.admit(cluster, "q3", "SELECT 1")
    assert(planned === 1)
  }

  test("an unrecognised statement is planned rather than assumed free") {
    var planned = 0
    val g = gate(explain = (_, _) => { planned += 1; planWith(GB) })
    g.admit(cluster, "q1", "MERGE INTO t USING s ON t.id = s.id")
    assert(planned === 1, "the safe direction for an unknown statement is to plan it")
  }

  test("a cluster with no declared capacity is admitted unaccounted, not refused") {
    val ungated = new AdmissionGate(
      new CapacityAccountant(AdmissionPolicy.PackByMemory),
      new QuerySizer(SizingPolicy(memoryFactor = 1.0)),
      _ => None,
      (_, _) => planWith(60 * GB))
    val admitted = ungated.admit(cluster, "q1", "SELECT 1").toOption.get
    assert(admitted.reservation.isEmpty,
      "refusing would break every cluster not yet annotated")
  }

  test("release returns the capacity to the pool") {
    val g = gate(explain = (_, _) => planWith(40 * GB))
    assert(g.admit(cluster, "q1", "SELECT 1").isRight)
    assert(g.admit(cluster, "q2", "SELECT 1").isLeft)
    g.release("c", "q1")
    assert(g.admit(cluster, "q2", "SELECT 1").isRight)
  }
}
