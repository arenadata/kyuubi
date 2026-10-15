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

package org.apache.kyuubi.it.hive.flight

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.UUID

import org.apache.kyuubi.{FlightSqlTestHelper, Utils, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.util.JavaUtils

class HiveFlightSqlSuite
  extends WithFlightSqlServer with FlightSqlTestHelper with HiveJDBCTestHelper {

  private val kyuubiHome: String =
    JavaUtils.getCodeSourceLocation(getClass).split("integration-tests").head

  private val testDirectory =
    Utils.createTempDir(prefix = getClass.getSimpleName).toAbsolutePath

  // Thrift first so getJdbcUrl resolves the JDBC frontend, not Flight.
  override protected val frontendProtocols =
    Seq(FrontendProtocols.THRIFT_BINARY, FrontendProtocols.FLIGHT_SQL)

  override protected val conf: KyuubiConf = {
    val metastore = testDirectory.resolve("metastore")
    KyuubiConf()
      .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME", kyuubiHome)
      .set(ENGINE_TYPE, "HIVE_SQL")
      // One shared engine so JDBC (OS user) and Flight (anonymous) share Derby metastore.
      .set(ENGINE_SHARE_LEVEL, "SERVER")
      .set(FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS, 10)
      .setIfMissing(ENGINE_IDLE_TIMEOUT, 30000L)
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$metastore;create=true")
      .set("hive.metastore.warehouse.dir", testDirectory.resolve("warehouse").toUri.toString)
      .set("fs.defaultFS", "file:///")
      // Prefer local fetch over Tez/MR for simple SELECTs used in these tests.
      .set("hive.fetch.task.conversion", "more")
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("Hive: Flight SQL reads JDBC-seeded rows") {
    val table = "flight_seed_" + UUID.randomUUID().toString.replace("-", "")
    val dataDirectory = Files.createDirectory(testDirectory.resolve(table))
    val sourceFile = testDirectory.resolve(s"$table.tsv")
    Files.write(sourceFile, "2\tsecond\n1\tfirst\n".getBytes(UTF_8))
    withJdbcStatement() { statement =>
      statement.execute(
        s"CREATE EXTERNAL TABLE $table (id BIGINT, label STRING) " +
          "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE " +
          s"LOCATION '${dataDirectory.toUri}'")
      statement.execute(s"LOAD DATA LOCAL INPATH '${sourceFile.toUri}' INTO TABLE $table")
    }
    try {
      withFlightSqlClient() { (_, sqlClient) =>
        val rows = executeAndCollect(sqlClient, s"SELECT id, label FROM $table")
          .sortBy(_(0).toString.toLong)
        assert(rows.size === 2)
        assert(rows(0)(0).toString.toLong === 1L)
        assert(rows(0)(1).toString === "first")
        assert(rows(1)(0).toString.toLong === 2L)
        assert(rows(1)(1).toString === "second")
      }
    } finally {
      withJdbcStatement() { statement =>
        statement.execute(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("Hive: multi-page stream through Flight SQL") {
    withFlightSqlClient() { (_, sqlClient) =>
      val sql =
        "SELECT cast(a.pos as int) AS id FROM (" +
          "SELECT posexplode(split(space(54), ' ')) AS (pos, val)) a"
      val pages = executeAndCollectPaged(sqlClient, sql)
      assert(pages.rows.size === 55)
      assertFlightExactPageSizes(pages.batchRowCounts, expectedTotalRows = 55, pageSize = 10)
    }
  }
}
