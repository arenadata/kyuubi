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

package org.apache.kyuubi.it.jdbc.postgresql

import java.sql.DriverManager
import java.util.UUID

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.jdbc.postgresql.WithPostgreSQLContainer
import org.apache.kyuubi.metrics.MetricsConf.METRICS_PROMETHEUS_PORT
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.util.JavaUtils

class PostgreSQLEngineProfileSuite extends WithKyuubiServer with WithPostgreSQLContainer
  with HiveJDBCTestHelper {

  private val profileName = "postgres_profile_it"
  private val kyuubiHome = JavaUtils.getCodeSourceLocation(getClass).split("integration-tests").head

  override protected val conf: KyuubiConf = {
    val serverConf = KyuubiConf()
    // Other JDBC engine suites set system properties. Keep their connection settings out of here.
    serverConf.getAll.keys.filter(_.startsWith("kyuubi.engine.jdbc.")).foreach(serverConf.unset)
    serverConf
      .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME", kyuubiHome)
      .set(ENGINE_TYPE, "SPARK_SQL")
      .set(METRICS_PROMETHEUS_PORT, 0)
      .set(ENGINE_IDLE_TIMEOUT, 60000L)
  }

  override def beforeAll(): Unit = {
    withContainers { container =>
      val profilePrefix = s"kyuubi.engine.profile.$profileName"
      conf.set(s"$profilePrefix.type", "JDBC")
        .set(s"$profilePrefix.conf.${ENGINE_JDBC_SHORT_NAME.key}", "postgresql")
        .set(s"$profilePrefix.conf.${ENGINE_JDBC_DRIVER_CLASS.key}", container.driverClassName)
        .set(s"$profilePrefix.conf.${ENGINE_JDBC_CONNECTION_URL.key}", container.jdbcUrl)
        .set(s"$profilePrefix.conf.${ENGINE_JDBC_CONNECTION_USER.key}", container.username)
        .set(s"$profilePrefix.conf.${ENGINE_JDBC_CONNECTION_PASSWORD.key}", container.password)
        .set(
          s"$profilePrefix.conf.${ENGINE_JDBC_EXTRA_CLASSPATH.key}",
          PostgreSQLTestUtils.jdbcConnectorPath(kyuubiHome))
      super.beforeAll()
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("engine profile routes to PostgreSQL and reads seeded data after reconnect") {
    withContainers { container =>
      val table = "profile_" + UUID.randomUUID().toString.replace("-", "")
      val backend = DriverManager.getConnection(
        container.jdbcUrl,
        container.username,
        container.password)
      try {
        val seed = backend.createStatement()
        try {
          seed.execute(s"CREATE TABLE $table (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
          seed.execute(s"INSERT INTO $table VALUES (2, 'second'), (1, 'first')")
          withSessionConf()(Map(ENGINE_PROFILE.key -> profileName))() {
            for (_ <- 1 to 2) {
              withJdbcStatement() { statement =>
                val identity = statement.executeQuery("SELECT current_database(), current_user")
                try {
                  assert(identity.next())
                  assert(identity.getString(1) === "postgres")
                  assert(identity.getString(2) === container.username)
                  assert(!identity.next())
                } finally {
                  identity.close()
                }
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
            }
          }
        } finally {
          try {
            seed.execute(s"DROP TABLE IF EXISTS $table")
          } finally {
            seed.close()
          }
        }
      } finally {
        backend.close()
      }
    }
  }
}
