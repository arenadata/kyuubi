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

class TrinoClusterQueriesSuite extends KyuubiFunSuite {

  private def query(
      id: String,
      state: String,
      tags: String,
      stats: String = """"peakUserMemoryReservation":1073741824"""): String =
    s"""{"queryId":"$id","state":"$state",
       | "session":{"user":"alice","source":"kyuubi","clientTags":[$tags]},
       | "queryStats":{$stats}}""".stripMargin

  test("reads the gateway's own queries out of the coordinator's list") {
    val observed = TrinoClusterQueries.parse(
      s"""[${query("20260911_1", "RUNNING", "\"kyuubi-reservation:r1\"")},
         |  ${query("20260911_2", "FINISHED", "\"kyuubi-reservation:r2\"")}]""".stripMargin)

    assert(observed.map(_.reservationId) === Seq("r1", "r2"))
    assert(observed.map(_.finished) === Seq(false, true))
    assert(observed.head.peakMemoryBytes === Some(1024L * 1024 * 1024))
  }

  test("queries nobody admitted through the gateway are ignored") {
    val observed = TrinoClusterQueries.parse(
      s"""[${query("20260911_1", "RUNNING", "")},
         |  ${query("20260911_2", "RUNNING", "\"team:analytics\"")}]""".stripMargin)
    assert(
      observed.isEmpty,
      "a query the gateway did not admit says nothing about the gateway's reservations")
  }

  test("the gateway's tag is found among the client's own") {
    val observed = TrinoClusterQueries.parse(
      s"""[${query("1", "RUNNING", "\"team:analytics\",\"kyuubi-reservation:r1\",\"adhoc\"")}]""")
    assert(observed.map(_.reservationId) === Seq("r1"))
  }

  test("an unknown state counts as still running") {
    val observed = TrinoClusterQueries.parse(
      s"""[${query("1", "WAITING_FOR_RESOURCES", "\"kyuubi-reservation:r1\"")}]""")
    assert(
      !observed.head.finished,
      "holding a reservation too long is safer than releasing memory still in use")
  }

  test("data sizes are read whether Trino spells them as bytes or as text") {
    assert(TrinoClusterQueries.parseDataSize("1024B") === Some(1024L))
    assert(TrinoClusterQueries.parseDataSize("1.5GB") === Some(1610612736L))
    assert(TrinoClusterQueries.parseDataSize("2kB") === Some(2048L))
    assert(TrinoClusterQueries.parseDataSize("what") === None)

    val textual = TrinoClusterQueries.parse(
      s"""[${query(
          "1",
          "FINISHED",
          "\"kyuubi-reservation:r1\"",
          """"peakUserMemoryReservation":"2GB"""")}]""")
    assert(textual.head.peakMemoryBytes === Some(2L * 1024 * 1024 * 1024))
  }

  test("a missing peak is absent rather than zero") {
    val observed = TrinoClusterQueries.parse(
      s"""[${query("1", "FINISHED", "\"kyuubi-reservation:r1\"", """"totalCpuTime":"1s"""")}]""")
    assert(
      observed.head.peakMemoryBytes.isEmpty,
      "zero would calibrate the estimate towards reserving nothing")
  }

  test("a response that is not a list of queries yields nothing, not an error") {
    assert(TrinoClusterQueries.parse("{}") === Seq.empty)
    assert(TrinoClusterQueries.parse("[]") === Seq.empty)
  }
}
