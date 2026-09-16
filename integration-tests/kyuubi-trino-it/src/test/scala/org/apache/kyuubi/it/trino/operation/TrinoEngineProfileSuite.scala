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

package org.apache.kyuubi.it.trino.operation

import java.sql.SQLException
import java.util.UUID

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.it.trino.WithKyuubiServerAndTrinoContainer
import org.apache.kyuubi.metrics.MetricsConf.METRICS_PROMETHEUS_PORT
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class TrinoEngineProfileSuite extends WithKyuubiServerAndTrinoContainer
  with HiveJDBCTestHelper {

  private val profileName = "trino_profile_it"

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME", kyuubiHome)
    .set(ENGINE_TYPE, "SPARK_SQL")
    .set(METRICS_PROMETHEUS_PORT, 0)
    .unset(ENGINE_TRINO_CONNECTION_URL)
    .unset(ENGINE_TRINO_CONNECTION_CATALOG)

  override protected def configureTrinoConnection(connectionUrl: String): Unit = {
    // Profiles are loaded once when the server starts; the container URL is only known here.
    conf.set(s"kyuubi.engine.profile.$profileName.type", "TRINO")
      .set(s"kyuubi.engine.profile.$profileName.session.engine.trino.connection.url", connectionUrl)
      .set(s"kyuubi.engine.profile.$profileName.session.engine.trino.connection.catalog", "memory")
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("engine profile routes JDBC queries to Trino and reads data after reconnect") {
    val schema = "profile_" + UUID.randomUUID().toString.replace("-", "")
    val table = s"memory.$schema.profile_rows"
    withSessionConf()(Map(ENGINE_PROFILE.key -> profileName))() {
      withJdbcStatement() { statement =>
        val result = statement.executeQuery("SELECT version(), current_catalog")
        try {
          assert(result.next())
          assert(result.getString(1) === IMAGE_VERSION.toString)
          assert(result.getString(2) === "memory")
          assert(!result.next())
        } finally {
          result.close()
        }
        statement.execute(s"CREATE SCHEMA memory.$schema")
      }
      try {
        withJdbcStatement() { statement =>
          statement.execute(s"CREATE TABLE $table (id BIGINT, label VARCHAR)")
          statement.execute(s"INSERT INTO $table VALUES (1, 'first'), (2, 'second')")
        }
        withJdbcStatement() { statement =>
          val result = statement.executeQuery(s"SELECT id, label FROM $table ORDER BY id")
          try {
            assert(result.next())
            assert(result.getLong("id") === 1L)
            assert(result.getString("label") === "first")
            assert(result.next())
            assert(result.getLong("id") === 2L)
            assert(result.getString("label") === "second")
            assert(!result.next())
          } finally {
            result.close()
          }
        }
      } finally {
        withJdbcStatement() { statement =>
          statement.execute(s"DROP TABLE IF EXISTS $table")
          statement.execute(s"DROP SCHEMA memory.$schema")
        }
      }
    }
  }

  test("client catalog overrides the profile for one Trino session") {
    val missingCatalog = "no_such_profile_catalog"
    val error = intercept[SQLException] {
      withSessionConf()(Map(
        ENGINE_PROFILE.key -> profileName,
        ENGINE_TRINO_CONNECTION_CATALOG.key -> missingCatalog))() {
        withJdbcStatement() { statement =>
          statement.executeQuery("SHOW SCHEMAS").close()
        }
      }
    }
    assert(error.getMessage.contains(s"Catalog '$missingCatalog' does not exist"))

    withSessionConf()(Map(ENGINE_PROFILE.key -> profileName))() {
      withJdbcStatement() { statement =>
        val schemas = statement.executeQuery("SHOW SCHEMAS")
        try {
          assert(schemas.next())
        } finally {
          schemas.close()
        }
        val catalog = statement.executeQuery("SELECT current_catalog")
        try {
          assert(catalog.next())
          assert(catalog.getString(1) === "memory")
          assert(!catalog.next())
        } finally {
          catalog.close()
        }
      }
    }
  }
}
