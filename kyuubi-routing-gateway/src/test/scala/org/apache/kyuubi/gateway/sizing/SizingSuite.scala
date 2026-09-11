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

package org.apache.kyuubi.gateway.sizing

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.gateway.capacity.ClusterCapacity

class SizingSuite extends KyuubiFunSuite {

  private val GB = 1024L * 1024 * 1024
  private val capacity = ClusterCapacity(10 * GB, workers = 2, maxWorkers = 10)

  // Shaped like Trino's EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON): a fragment map
  // whose nodes carry `estimates` and nest through `children`.
  private val plan =
    """{
      |  "0": {
      |    "id": "10", "name": "Output", "descriptor": {}, "outputs": [], "details": [],
      |    "estimates": [
      |      {"outputRowCount": 100.0, "outputSizeInBytes": 1000.0,
      |       "cpuCost": 500.0, "memoryCost": 1073741824.0, "networkCost": 0.0}
      |    ],
      |    "children": [
      |      {
      |        "id": "11", "name": "Aggregate", "descriptor": {}, "outputs": [], "details": [],
      |        "estimates": [
      |          {"outputRowCount": 5000.0, "outputSizeInBytes": 50000.0,
      |           "cpuCost": 1500.0, "memoryCost": 21474836480.0, "networkCost": 0.0}
      |        ],
      |        "children": []
      |      }
      |    ]
      |  }
      |}""".stripMargin

  test("finds estimates anywhere in the document, not at a fixed path") {
    val e = ExplainParser.parse(plan)
    assert(e.estimatesPresent)
    assert(e.peakMemoryBytes === 20 * GB, "peak is the largest single operator")
    assert(e.totalMemoryBytes === 21 * GB, "total sums the operators")
    assert(e.cpuCost === 2000.0)
    assert(e.outputRowCount === 5000.0)
  }

  test("NaN estimates mean unknown, not zero") {
    // Jackson writes an unestimatable double as the quoted string "NaN", which
    // is what Trino emits when table statistics are missing.
    val quoted =
      """{"id":"1","estimates":[{"outputRowCount":"NaN","cpuCost":"NaN","memoryCost":"NaN"}],
        |"children":[]}""".stripMargin
    assert(!ExplainParser.parse(quoted).estimatesPresent,
      "missing table statistics must not read as a free query")

    // A bare NaN token is not valid JSON; losing one field to it is acceptable,
    // losing the whole plan is not.
    val bare =
      """{"id":"1","estimates":[{"memoryCost":NaN,"cpuCost":1500.0}],"children":[]}"""
    val e = ExplainParser.parse(bare)
    assert(e.estimatesPresent, "the readable fields of the document survive")
    assert(e.cpuCost === 1500.0)
    assert(e.peakMemoryBytes === 0L)
  }

  test("malformed output is unknown rather than an exception") {
    assert(!ExplainParser.parse("not json at all").estimatesPresent)
    assert(!ExplainParser.parse("{}").estimatesPresent)
  }

  test("memory sets a floor on the worker count") {
    val sizer = new QuerySizer(SizingPolicy(memoryFactor = 1.0))
    val d = sizer.size(ExplainParser.parse(plan), capacity)
    assert(d.workers === 2, "20 GB over 10 GB nodes needs two workers")
    assert(d.queryMemoryBytes === 20 * GB)
  }

  test("the calibration factor scales the requirement") {
    val sizer = new QuerySizer(SizingPolicy(memoryFactor = 2.0))
    assert(sizer.size(ExplainParser.parse(plan), capacity).workers === 4)
  }

  test("no estimate falls back to the default, not to one worker") {
    val sizer = new QuerySizer(SizingPolicy(defaultWorkers = 3))
    val d = sizer.size(QueryEstimate.unknown, capacity)
    assert(d.workers === 3)
    assert(d.reason.contains("no planner estimate"))
  }

  test("the speed target can only raise the floor, and is off by default") {
    val estimate = ExplainParser.parse(plan)
    assert(new QuerySizer(SizingPolicy(memoryFactor = 1.0)).size(estimate, capacity).workers === 2)

    // 2000 units of work in 100s needs 20 cores, i.e. 5 workers of 4 cores.
    val fast = new QuerySizer(SizingPolicy(
      memoryFactor = 1.0,
      targetSecondsPerCpu = 100,
      coresPerWorker = 4))
    val d = fast.size(estimate, capacity)
    assert(d.workers === 5)
    assert(d.reason.contains("speed target"))
  }

  test("the ceiling is never exceeded") {
    val small = ClusterCapacity(1 * GB, workers = 1, maxWorkers = 3)
    val d = new QuerySizer(SizingPolicy(memoryFactor = 1.0)).size(ExplainParser.parse(plan), small)
    assert(d.workers === 3, "20 GB would want 20 workers, the cluster allows 3")
  }
}
