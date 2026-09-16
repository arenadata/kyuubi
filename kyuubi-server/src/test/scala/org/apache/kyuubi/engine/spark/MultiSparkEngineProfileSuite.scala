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

package org.apache.kyuubi.engine.spark

import java.nio.file.{Files, Path, Paths}
import java.sql.{Connection, DriverManager, ResultSet}

import scala.collection.JavaConverters._

import org.scalatest.DoNotDiscover

import org.apache.kyuubi.{KYUUBI_VERSION, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.spark.SparkProcessBuilder.{SPARK_CORE_SCALA_VERSION_REGEX, SPARK_CORE_VERSION_REGEX}
import org.apache.kyuubi.metrics.MetricsConf.METRICS_PROMETHEUS_PORT
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/** Run explicitly after preparing both Spark distributions and engine jars; see testing.md. */
@DoNotDiscover
class MultiSparkEngineProfileSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(ENGINE_SHARE_LEVEL, "USER")
    .set(METRICS_PROMETHEUS_PORT, 0)
  override protected def jdbcUrl: String = getJdbcUrl

  private var fixtureRoot: Path = _

  override def beforeAll(): Unit = {
    // ScalaTest Maven runs from the server module and supplies its absolute basedir.
    val moduleRoot = Paths.get(System.getProperty("basedir", ".")).toAbsolutePath.normalize()
    val projectRoot = moduleRoot.getParent
    val homes = Seq(
      ("spark3", "3.5", requiredHome("kyuubi.test.spark3.home")),
      ("spark4", "4.2", requiredHome("kyuubi.test.spark4.home")))
    homes.foreach { case (_, version, home) => validateSparkHome(home, version, projectRoot) }

    fixtureRoot = Utils.createTempDir("multi-spark-profiles")
    homes.foreach { case (profile, _, home) =>
      val confDir = Files.createDirectory(fixtureRoot.resolve(profile))
      val prefix = s"kyuubi.engine.profile.$profile"
      conf.set(s"$prefix.type", "SPARK_SQL")
      conf.set(s"$prefix.env.SPARK_HOME", home.toString)
      conf.set(s"$prefix.env.SPARK_CONF_DIR", confDir.toString)
      conf.set(s"$prefix.env.KYUUBI_HOME", projectRoot.toString)
      conf.set(s"$prefix.conf.spark.master", "local[2]")
      conf.set(s"$prefix.conf.spark.sql.catalogImplementation", "in-memory")
      conf.set(s"$prefix.conf.spark.sql.shuffle.partitions", "2")
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      // Prerequisite validation can fail before WithKyuubiServer starts its server.
      if (server != null) {
        super.afterAll()
      }
    } finally {
      if (fixtureRoot != null) {
        Utils.deleteDirectoryRecursively(fixtureRoot.toFile)
      }
    }
  }

  test("Spark 3 and Spark 4 profiles isolate and reuse live USER engines") {
    withProfileConnection("spark3") { spark3 =>
      val spark3Id = assertEngine(spark3, "3.5")
      withProfileConnection("spark4") { spark4 =>
        val spark4Id = assertEngine(spark4, "4.2")
        assert(spark3Id != spark4Id, "Different profiles must not share the USER engine")
        assert(assertEngine(spark3, "3.5") === spark3Id)

        withProfileConnection("spark3") { anotherSpark3 =>
          assert(assertEngine(anotherSpark3, "3.5") === spark3Id)
        }
        withProfileConnection("spark4") { anotherSpark4 =>
          assert(assertEngine(anotherSpark4, "4.2") === spark4Id)
        }

        spark3.close()
        assert(assertEngine(spark4, "4.2") === spark4Id)
      }
    }
  }

  private def requiredHome(property: String): Path = {
    // ScalaTest Maven can stringify an unset forwarded property as the literal "null".
    val value =
      sys.props.get(property).map(_.trim).filter(v => v.nonEmpty && v != "null").getOrElse {
        throw new IllegalArgumentException(s"Set -D$property to the required Spark distribution")
      }
    val home = Paths.get(value)
    require(home.isAbsolute, s"-D$property must be an absolute path: $home")
    home.normalize()
  }

  private def validateSparkHome(home: Path, version: String, projectRoot: Path): Unit = {
    val executable = home.resolve("bin/spark-submit")
    require(
      Files.isRegularFile(executable) && Files.isExecutable(executable),
      s"Spark $version executable is missing or not executable: $executable")
    val jarsDir = home.resolve("jars")
    require(Files.isDirectory(jarsDir), s"Spark jars directory is missing: $jarsDir")
    val files = Files.list(jarsDir)
    val jars =
      try {
        files.iterator().asScala.map(_.getFileName.toString).toVector
      } finally {
        files.close()
      }
    val versions = jars.collect { case SPARK_CORE_VERSION_REGEX(found) => found }
    val scalaVersions = jars.collect { case SPARK_CORE_SCALA_VERSION_REGEX(found) => found }
    require(versions == Seq(version), s"Expected one Spark $version core jar in $home: $versions")
    require(scalaVersions == Seq("2.13"), s"Expected Scala 2.13 in $home: $scalaVersions")
    val engineJar = projectRoot.resolve("externals/kyuubi-spark-sql-engine/target")
      .resolve(s"kyuubi-spark-sql-engine-spark-${version}_2.13-$KYUUBI_VERSION.jar")
    require(
      Files.isRegularFile(engineJar) && Files.isReadable(engineJar),
      s"Build the matching Spark $version engine before running this suite: $engineJar")
  }

  private def withProfileConnection[T](profile: String)(f: Connection => T): T = {
    val connection = DriverManager.getConnection(
      s"$jdbcUrl?kyuubi.engine.profile=$profile",
      user,
      password)
    try f(connection)
    finally connection.close()
  }

  private def assertEngine(connection: Connection, expectedVersion: String): String = {
    val engineId = withQuery(connection, "SELECT version(), engine_id()") { rows =>
      assert(rows.next())
      assert(rows.getString(1).startsWith(s"$expectedVersion."))
      val id = rows.getString(2)
      assert(id != null && id.nonEmpty)
      assert(!rows.next())
      id
    }
    withQuery(connection, "SELECT sum(id) FROM range(0, 10, 1, 2)") { rows =>
      assert(rows.next())
      assert(rows.getLong(1) === 45L)
      assert(!rows.next())
    }
    engineId
  }

  private def withQuery[T](connection: Connection, sql: String)(f: ResultSet => T): T = {
    val statement = connection.createStatement()
    try {
      val rows = statement.executeQuery(sql)
      try f(rows)
      finally rows.close()
    } finally {
      statement.close()
    }
  }
}
