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

package org.apache.kyuubi.gateway.it

import java.net.URI
import java.time.ZoneId
import java.util.{Locale, Optional}

import scala.collection.JavaConverters._

import io.trino.client.ClientSession
import okhttp3.OkHttpClient

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.TrinoContext
import org.apache.kyuubi.gateway.sizing.{ExplainParser, TrinoQueryPlanner}
import org.apache.kyuubi.gateway.tags.ContainerTest
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/**
 * Admission against a real planner.
 *
 * Sizing has been exercised against plans written by hand, which proves the
 * parser and proves nothing about the plans. Only a real coordinator can say
 * whether `EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)` on a real query produces a
 * document with estimates in it - and if it does not, every query sizes as
 * unknown and the gate is a very elaborate way of doing nothing.
 */
@ContainerTest
class GatewayAdmissionSuite extends HiveJDBCTestHelper with WithGatewayAndTrinoContainer {

  override protected val catalog = "tpch"
  override protected val schema = "tiny"

  override protected def jdbcUrl: String = binaryUrl

  override protected def gatewayConf(trinoUrl: String): KyuubiConf =
    baseConf(trinoUrl)
      .set("kyuubi.gateway.admission.enabled", "true")
      .set("kyuubi.gateway.sizing.memoryFactor", "1.5")
      // The container is one node, so a query told to wait for two workers
      // waits for a second one that will never register.
      .set("kyuubi.gateway.sizing.defaultWorkers", "1")
      // A ceiling high enough that nothing here is refused for being too large:
      // what is under test is that admission works, not where its limits are.
      .set("kyuubi.gateway.cluster.analytics.max-memory-per-node-bytes", "1073741824")
      .set(
        "kyuubi.gateway.cluster.analytics.session." +
          KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG.key,
        "tpch")

  private def planner: TrinoQueryPlanner = {
    val session = ClientSession.builder()
      .server(URI.create(trinoUrl))
      .principal(Optional.of("alice"))
      .source("kyuubi")
      .catalog(catalog)
      .schema(schema)
      .timeZone(ZoneId.of("UTC"))
      .locale(Locale.ENGLISH)
      .properties(Map.empty[String, String].asJava)
      .build()
    new TrinoQueryPlanner(
      TrinoContext(new OkHttpClient.Builder().build(), session),
      KyuubiConf())
  }

  test("a real distributed plan carries the estimates sizing needs") {
    val plan = planner.explain("SELECT * FROM tpch.tiny.orders WHERE totalprice > 100")

    assert(plan.nonEmpty, "the coordinator returned no plan")
    val estimate = ExplainParser.parse(plan)
    assert(
      estimate.estimatesPresent,
      s"a plan with no estimates sizes every query as unknown; got: ${plan.take(400)}")
    // A scan holds nothing, so the planner reports memoryCost 0 truthfully -
    // which is exactly the case that used to reserve nothing and admit
    // everything. What it does report is the data that has to move.
    assert(
      estimate.peakOutputBytes > 0,
      s"a query that reserves nothing makes the gate decorative; got $estimate")
  }

  test("a query asked to wait for workers that do not exist fails visibly") {
    // Proves required_workers_count actually reaches the cluster. The container
    // is a single node; asking for two means the query is held rather than run,
    // and a short wait turns that into an error instead of a hang.
    withSessionConf()(Map.empty)(Map.empty) {
      val refused = intercept[Exception] {
        withJdbcStatement() { statement =>
          statement.execute("SET SESSION required_workers_count = 2")
          statement.execute("SET SESSION required_workers_max_wait_time = '5s'")
          statement.executeQuery("SELECT count(*) FROM tpch.tiny.nation")
        }
      }
      assert(
        refused.getMessage.contains("nodes") || refused.getMessage.contains("workers"),
        s"the failure should name the missing workers, got: ${refused.getMessage}")
    }
  }

  test("a query whose size cannot be known is still sized, from statistics") {
    // tpch generates statistics, so this is a plan the optimiser can cost -
    // which is the case the memory factor is calibrated for.
    val estimate = ExplainParser.parse(
      planner.explain("SELECT custkey, sum(totalprice) FROM tpch.tiny.orders GROUP BY custkey"))
    assert(estimate.estimatesPresent)
    assert(estimate.outputRowCount > 0, s"a row count of zero says the plan was not costed")
  }

  test("an admitted query runs and returns the right answer") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT count(*) AS c FROM tpch.tiny.nation")
      assert(resultSet.next())
      // tpch.tiny.nation is 25 rows in every Trino build.
      assert(resultSet.getLong("c") === 25L)
    }
  }

  test("statements that touch no data are admitted without being planned") {
    withJdbcStatement() { statement =>
      // A SHOW is on the no-sizing list. If it were planned, this would fail:
      // EXPLAIN of SHOW is not something the optimiser costs.
      val resultSet = statement.executeQuery("SHOW SCHEMAS FROM tpch")
      assert(resultSet.next(), "a metadata statement must still return its rows")
    }
  }
}
