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

package org.apache.kyuubi.it.trino.flight

import java.util.UUID

import org.apache.kyuubi.FlightSqlTestHelper
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.it.trino.WithKyuubiServerAndTrinoContainer
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.server.KyuubiFlightSqlFrontendService

class TrinoFlightSqlSuite
  extends WithKyuubiServerAndTrinoContainer with FlightSqlTestHelper with HiveJDBCTestHelper {

  override protected val frontendProtocols =
    Seq(FrontendProtocols.THRIFT_BINARY, FrontendProtocols.FLIGHT_SQL)

  override def beforeAll(): Unit = {
    conf.set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
    conf.set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)
    conf.set(FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS, 10)
    conf.set(ENGINE_TRINO_EVENT_LOGGERS, Seq.empty)
    super.beforeAll()
  }

  override protected def jdbcUrl: String = getJdbcUrl

  override protected def flightSqlUrl: String =
    server.frontendServices.collectFirst {
      case frontend: KyuubiFlightSqlFrontendService => frontend.connectionUrl
    }.getOrElse(throw new IllegalStateException("Flight SQL frontend is not running"))

  test("Trino: Flight SQL reads JDBC-seeded rows") {
    val schema = "flight_seed_" + UUID.randomUUID().toString.replace("-", "")
    val table = s"memory.$schema.seed_rows"
    withJdbcStatement() { statement =>
      statement.execute(s"CREATE SCHEMA memory.$schema")
    }
    try {
      withJdbcStatement() { statement =>
        statement.execute(s"CREATE TABLE $table (id BIGINT, label VARCHAR)")
        statement.execute(s"INSERT INTO $table VALUES (1, 'first'), (2, 'second')")
      }
      withFlightSqlClient() { (_, sqlClient) =>
        val rows = executeAndCollect(
          sqlClient,
          s"SELECT id, label FROM $table ORDER BY id")
        assert(rows.size === 2)
        assert(rows(0)(0).toString.toLong === 1L)
        assert(rows(0)(1).toString === "first")
        assert(rows(1)(0).toString.toLong === 2L)
        assert(rows(1)(1).toString === "second")
      }
    } finally {
      withJdbcStatement() { statement =>
        statement.execute(s"DROP TABLE IF EXISTS $table")
        statement.execute(s"DROP SCHEMA memory.$schema")
      }
    }
  }

  test("Trino: multi-page stream through Flight SQL") {
    withFlightSqlClient() { (_, sqlClient) =>
      val pages = executeAndCollectPaged(
        sqlClient,
        "SELECT x FROM UNNEST(SEQUENCE(0, 54)) AS t(x)")
      assert(pages.rows.size === 55)
      assertFlightExactPageSizes(pages.batchRowCounts, expectedTotalRows = 55, pageSize = 10)
    }
  }
}
