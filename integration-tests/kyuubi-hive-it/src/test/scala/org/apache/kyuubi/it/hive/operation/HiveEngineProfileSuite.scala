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

package org.apache.kyuubi.it.hive.operation

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.metrics.MetricsConf.METRICS_PROMETHEUS_PORT
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.util.JavaUtils

class HiveEngineProfileSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  private val profileName = "hive_profile_it"
  private val profilePrefix = s"kyuubi.engine.profile.$profileName"
  private val compressionKey = "hive.exec.compress.output"
  private val testDirectory = Utils.createTempDir(prefix = getClass.getSimpleName).toAbsolutePath
  private val kyuubiHome = JavaUtils.getCodeSourceLocation(getClass).split("integration-tests").head

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME", kyuubiHome)
    .set(ENGINE_TYPE, "SPARK_SQL")
    .set(METRICS_PROMETHEUS_PORT, 0)
    .set(ENGINE_SHARE_LEVEL, "USER")
    .set(ENGINE_IDLE_TIMEOUT, 30000L)
    .set(s"$profilePrefix.type", "HIVE_SQL")
    .set(s"$profilePrefix.conf.${ENGINE_HIVE_DEPLOY_MODE.key}", "local")
    .set(
      s"$profilePrefix.conf.javax.jdo.option.ConnectionURL",
      s"jdbc:derby:;databaseName=${testDirectory.resolve("metastore")};create=true")
    .set(
      s"$profilePrefix.conf.hive.metastore.warehouse.dir",
      testDirectory.resolve("warehouse").toUri.toString)
    .set(s"$profilePrefix.conf.fs.defaultFS", "file:///")
    .set(s"$profilePrefix.conf.hive.fetch.task.conversion", "more")
    .set(s"$profilePrefix.conf.$compressionKey", "true")

  override protected def jdbcUrl: String = getJdbcUrl

  test("engine profile routes JDBC queries to Hive and loads local data before reconnect") {
    withSessionConf()(Map(
      ENGINE_PROFILE.key -> profileName,
      SERVER_INFO_PROVIDER.key -> "ENGINE"))() {
      withJdbcStatement() { statement =>
        eventually(timeout(30.seconds), interval(1.second)) {
          assert(statement.getConnection.getMetaData.getDatabaseProductName === "Apache Hive")
        }
      }
    }

    val table = "profile_" + UUID.randomUUID().toString.replace("-", "")
    val dataDirectory = Files.createDirectory(testDirectory.resolve(table))
    val sourceFile = testDirectory.resolve(s"$table.tsv")
    Files.write(sourceFile, "2\tsecond\n1\tfirst\n".getBytes(UTF_8))
    withSessionConf()(Map(ENGINE_PROFILE.key -> profileName))() {
      try {
        withJdbcStatement() { statement =>
          statement.execute(
            s"CREATE EXTERNAL TABLE $table (id BIGINT, label STRING) " +
              "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE " +
              s"LOCATION '${dataDirectory.toUri}'")
          // Write through Hive's SQL path into the initially empty table location.
          // A nonpartitioned, unbucketed text table uses LOAD's local copy/move task.
          statement.execute(s"LOAD DATA LOCAL INPATH '${sourceFile.toUri}' INTO TABLE $table")
        }
        withJdbcStatement() { statement =>
          // Sort in the client so this test only needs Hive's local fetch task, not MapReduce.
          val result = statement.executeQuery(s"SELECT id, label FROM $table")
          try {
            val rows = ArrayBuffer.empty[(Long, String)]
            while (result.next()) {
              rows += ((result.getLong("id"), result.getString("label")))
            }
            assert(rows.sortBy(_._1).toSeq === Seq(1L -> "first", 2L -> "second"))
          } finally {
            result.close()
          }
        }
      } finally {
        withJdbcStatement() { statement =>
          statement.execute(s"DROP TABLE IF EXISTS $table")
        }
      }
    }
  }

  test("client Hive configuration overrides the profile for one session") {
    withSessionConf()(Map(ENGINE_PROFILE.key -> profileName))() {
      assertCompressionSetting("true")
    }
    withSessionConf()(Map(
      ENGINE_PROFILE.key -> profileName,
      compressionKey -> "false"))() {
      assertCompressionSetting("false")
    }
    withSessionConf()(Map(ENGINE_PROFILE.key -> profileName))() {
      assertCompressionSetting("true")
    }
  }

  private def assertCompressionSetting(expected: String): Unit = {
    withJdbcStatement() { statement =>
      val result = statement.executeQuery(s"SET $compressionKey")
      try {
        assert(result.next())
        assert(result.getString(1) === s"$compressionKey=$expected")
        assert(!result.next())
      } finally {
        result.close()
      }
    }
  }
}
