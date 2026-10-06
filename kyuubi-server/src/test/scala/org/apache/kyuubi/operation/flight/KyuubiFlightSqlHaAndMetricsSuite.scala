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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.flight.CallStatus
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{FlightSqlTestHelper, Utils, WithFlightSqlHaCluster}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_FLIGHT_SQL_NAMESPACE, HA_NAMESPACE}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider
import org.apache.kyuubi.metrics.{MetricsConf, MetricsConstants, ReporterType}

class KyuubiFlightSqlHaAndMetricsSuite extends WithFlightSqlHaCluster with FlightSqlTestHelper {

  val reportPath: Path = Utils.createTempDir()

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(ENGINE_SHARE_LEVEL, "connection")
      .set(HA_FLIGHT_SQL_NAMESPACE, "kyuubi_flight_ha_it")
      .set(HA_NAMESPACE, "kyuubi_thrift_ha_it")
      .set(MetricsConf.METRICS_ENABLED, true)
      .set(MetricsConf.METRICS_REPORTERS, Set(ReporterType.JSON.toString))
      .set(MetricsConf.METRICS_JSON_LOCATION, reportPath.toString)
      .set(MetricsConf.METRICS_JSON_INTERVAL, Duration.ofMillis(100).toMillis)
  }

  override protected def flightSqlUrl: String = flightSqlUrl(servers.head)

  test("HA: Flight SQL registers under dedicated ZK namespace") {
    val namespace = "/" + flightSqlNamespace.stripPrefix("/")
    val thriftNamespace = "/" + conf.get(HA_NAMESPACE).stripPrefix("/")
    DiscoveryClientProvider.withDiscoveryClient(conf) { client =>
      val nodes = client.getServiceNodesInfo(namespace)
      assert(nodes.size === serverCount, s"expected $serverCount nodes under $namespace")
      val expected = servers.map(flightSqlUrl).toSet
      val registered = nodes.map(n => s"${n.host}:${n.port}").toSet
      assert(registered === expected)
      assert(
        client.pathNonExists(thriftNamespace) ||
          client.getChildren(thriftNamespace).isEmpty,
        s"Flight SQL endpoints must not register under Thrift namespace $thriftNamespace")
    }
  }

  test("HA: stream against peer node returns NOT_FOUND for node-local ticket") {
    val owner = servers.head
    val peer = peerOf(owner)
    val ticket = withFlightSqlClientAt(flightSqlUrl(owner)) { (_, sqlClient) =>
      val info = sqlClient.execute("SELECT 1")
      assert(info.getEndpoints.size() === 1)
      info.getEndpoints.get(0).getTicket
    }
    withFlightSqlClientAt(flightSqlUrl(peer)) { (_, sqlClient) =>
      assertFlightStatus(CallStatus.NOT_FOUND) {
        readTicketRows(sqlClient, ticket)
      }
    }
  }

  test("metrics: operation and stream counters are emitted") {
    val counters_name = "counters"
    val meters_name = "meters"
    val gauges_name = "gauges"
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
      assert(report.has(counters_name) || report.has(meters_name) || report.has(gauges_name))
      val counters =
        if (report.has(counters_name)) report.get(counters_name)
        else report.path(counters_name)
      val meters =
        if (report.has(meters_name)) report.get(meters_name)
        else report.path(meters_name)
      val gauges =
        if (report.has(gauges_name)) report.get(gauges_name)
        else report.path(gauges_name)

      def countOf(name: String): Long = {
        if (counters.has(name)) counters.get(name).get("count").asLong()
        else if (gauges.has(name)) gauges.get(name).get("value").asLong()
        else if (meters.has(name)) meters.get(name).get("count").asLong()
        else 0L
      }

      assert(countOf(MetricsConstants.FLIGHT_SQL_OPERATION_TOTAL) >= 1)
      assert(countOf(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL) >= 1)
      assert(countOf(MetricsConstants.FLIGHT_SQL_STREAM_ROWS) >= 3)
      assert(countOf(MetricsConstants.FLIGHT_SQL_STREAM_BATCHES) >= 1)
    }
  }
}
