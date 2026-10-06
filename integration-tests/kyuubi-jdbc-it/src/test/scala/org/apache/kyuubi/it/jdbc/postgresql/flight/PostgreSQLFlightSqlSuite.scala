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

package org.apache.kyuubi.it.jdbc.postgresql.flight

import java.sql.DriverManager
import java.util.UUID

import org.apache.kyuubi.FlightSqlTestHelper
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.it.jdbc.postgresql.WithKyuubiServerAndPostgreSQLContainer
import org.apache.kyuubi.server.KyuubiFlightSqlFrontendService

class PostgreSQLFlightSqlSuite
  extends WithKyuubiServerAndPostgreSQLContainer with FlightSqlTestHelper {

  override protected val frontendProtocols = Seq(FrontendProtocols.FLIGHT_SQL)

  override def beforeAll(): Unit = {
    FlightSqlTestHelper.ensureArrowUnsafeAllocator()
    conf.set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
    conf.set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)
    conf.set(FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS, 10)
    super.beforeAll()
  }

  override protected def flightSqlUrl: String =
    server.frontendServices.collectFirst {
      case frontend: KyuubiFlightSqlFrontendService => frontend.connectionUrl
    }.getOrElse(throw new IllegalStateException("Flight SQL frontend is not running"))

  test("PostgreSQL: Flight SQL reads container-seeded rows") {
    val table = "flight_seed_" + UUID.randomUUID().toString.replace("-", "")
    val url = conf.get(ENGINE_JDBC_CONNECTION_URL).get
    val user = conf.get(ENGINE_JDBC_CONNECTION_USER).get
    val password = conf.get(ENGINE_JDBC_CONNECTION_PASSWORD).get
    val backend = DriverManager.getConnection(url, user, password)
    try {
      val seed = backend.createStatement()
      try {
        seed.execute(s"CREATE TABLE $table (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
        seed.execute(s"INSERT INTO $table VALUES (2, 'second'), (1, 'first')")
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
        try seed.execute(s"DROP TABLE IF EXISTS $table")
        finally seed.close()
      }
    } finally {
      backend.close()
    }
  }

  test("PostgreSQL: multi-page stream through Flight SQL") {
    withFlightSqlClient() { (_, sqlClient) =>
      val pages = executeAndCollectPaged(
        sqlClient,
        "SELECT generate_series(0, 54) AS id")
      assert(pages.rows.size === 55)
      assertFlightExactPageSizes(pages.batchRowCounts, expectedTotalRows = 55, pageSize = 10)
    }
  }
}
