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

package org.apache.kyuubi.operation.flight

import java.nio.file.{Path, Paths}
import java.time.Duration

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{FlightSqlTestHelper, Utils, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.metrics.{MetricsConf, MetricsConstants, ReporterType}

class KyuubiFlightSqlMetricsSuite extends WithFlightSqlServer with FlightSqlTestHelper {

  val reportPath: Path = Utils.createTempDir()

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(ENGINE_SHARE_LEVEL, "connection")
      .set(MetricsConf.METRICS_REPORTERS, Set(ReporterType.JSON.toString))
      .set(MetricsConf.METRICS_JSON_LOCATION, reportPath.toString)
      .set(MetricsConf.METRICS_JSON_INTERVAL, Duration.ofMillis(100).toMillis)
  }

  test("metrics: operation and stream counters are emitted") {
    val mapper = new ObjectMapper()
    withFlightSqlClient() { (_, sqlClient) =>
      val rows = executeAndCollect(sqlClient, "SELECT id FROM range(3)")
      assert(rows.size === 3)
      intercept[Exception] {
        sqlClient.execute("SELECT * FROM flight_sql_metrics_missing_table")
      }
    }

    eventually(timeout(20.seconds), interval(1.second)) {
      val report = mapper.readTree(Paths.get(reportPath.toString, "report.json").toFile)
      assertAtLeast(report, "counters", MetricsConstants.FLIGHT_SQL_OPERATION_TOTAL, 1)
      assertAtLeast(report, "counters", MetricsConstants.FLIGHT_SQL_OPERATION_FAIL, 1)
      assertAtLeast(report, "meters", MetricsConstants.FLIGHT_SQL_STREAM_ROWS, 3)
      assertAtLeast(report, "meters", MetricsConstants.FLIGHT_SQL_STREAM_BATCHES, 1)
    }
  }

  private def assertAtLeast(
      report: JsonNode,
      section: String,
      name: String,
      atLeast: Long): Unit = {
    val count = report.path(section).path(name).path("count")
    val actual = if (count.isNumber) count.asLong().toString else "missing"
    assert(
      count.isNumber && count.asLong() >= atLeast,
      s"$section.$name count is $actual, expected >= $atLeast")
  }
}
