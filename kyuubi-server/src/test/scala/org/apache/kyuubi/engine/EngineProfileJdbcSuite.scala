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

package org.apache.kyuubi.engine

import java.sql.{Connection, DriverManager, SQLException}
import java.util.{Map => JMap}

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_PROFILE_NAME_KEY
import org.apache.kyuubi.metrics.MetricsConf.METRICS_PROMETHEUS_PORT
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.plugin.GroupProvider

private[engine] object EngineProfileJdbcTest {
  val PROFILE_USER = "profile_user"
  val OTHER_USER = "profile_other"
  val FIRST_GROUP = "profile_first"
  val SECOND_GROUP = "profile_second"
  val MARKER = "spark.sql.kyuubi.profile.marker"
  val IGNORED_MARKER = "spark.sql.kyuubi.profile.ignored"
  val RESTRICTED_KEY = "spark.sql.shuffle.partitions"
  val TYPE_DEFAULT = "kyuubi.engine.SPARK_SQL.profile.default"

  def principalKey(principal: String, key: String): String = s"___${principal}___.$key"
}

/** Keep group ordering deterministic without changing Hadoop's process-wide UGI state. */
class EngineProfileTestGroupProvider extends GroupProvider {
  import EngineProfileJdbcTest._

  override def groups(user: String, sessionConf: JMap[String, String]): Array[String] = {
    if (user == PROFILE_USER) Array(FIRST_GROUP, SECOND_GROUP) else Array(user)
  }

  override def primaryGroup(user: String, sessionConf: JMap[String, String]): String =
    groups(user, sessionConf).head
}

private[engine] trait EngineProfileJdbcTest extends WithKyuubiServer with HiveJDBCTestHelper {
  import EngineProfileJdbcTest._

  protected def unknownStrategy: String = "FAIL"

  override protected lazy val conf: KyuubiConf = {
    val result = KyuubiConf()
      .set(METRICS_PROMETHEUS_PORT, 0)
      .set(ENGINE_DO_AS_ENABLED, false)
      .set(ENGINE_SHARE_LEVEL, "USER")
      .set(ENGINE_IDLE_TIMEOUT, 60000L)
      .set(GROUP_PROVIDER, classOf[EngineProfileTestGroupProvider].getName)
      .set(ENGINE_PROFILES_UNKNOWN_STRATEGY, unknownStrategy)
      .set(SESSION_CONF_IGNORE_LIST, Set(IGNORED_MARKER))
      .set(SESSION_CONF_RESTRICT_LIST, Set(RESTRICTED_KEY))
      .set("spark.master", "local[1]")
      .set("spark.driver.memory", "512m")
      .set(MARKER, "legacy")
    Seq("alpha", "beta", "gamma").foreach { profile =>
      result.set(s"kyuubi.engine.profile.$profile.type", "SPARK_SQL")
      result.set(s"kyuubi.engine.profile.$profile.conf.$MARKER", profile)
      result.set(s"kyuubi.engine.profile.$profile.conf.$IGNORED_MARKER", profile)
      result.set(s"kyuubi.engine.profile.$profile.session.idle.timeout", "PT2M")
    }
    result.set("kyuubi.engine.profile.declared_trino.type", "TRINO")
    result.set(s"kyuubi.engine.profile.declared_trino.conf.$MARKER", "declared_trino")
    result
  }

  override protected def jdbcUrl: String = getJdbcUrl

  protected def withServerConf(settings: (String, String)*)(f: => Unit): Unit = {
    val previous = settings.map { case (key, _) => key -> conf.getOption(key) }
    settings.foreach { case (key, value) => conf.set(key, value) }
    try f
    finally {
      previous.foreach {
        case (key, Some(value)) => conf.set(key, value)
        case (key, None) => conf.unset(key)
      }
    }
  }

  protected def openConnection(
      settings: Map[String, String] = Map.empty,
      username: String = PROFILE_USER): Connection = {
    // Generic Kyuubi settings must use JDBC hiveconf (?key=value), not session variables.
    withSessionConf(Map.empty)(settings)(Map.empty) {
      DriverManager.getConnection(jdbcUrlWithConf, username, password)
    }
  }

  protected def withConnection(
      settings: Map[String, String] = Map.empty,
      username: String = PROFILE_USER)(f: Connection => Unit): Unit = {
    val connection = openConnection(settings, username)
    try f(connection)
    finally connection.close()
  }

  protected def queryValue(connection: Connection, sql: String, column: Int = 1): String = {
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(sql)
      try {
        assert(result.next(), s"No result for $sql")
        val value = result.getString(column)
        assert(!result.next(), s"More than one result for $sql")
        value
      } finally result.close()
    } finally statement.close()
  }

  protected def assertProfile(
      expected: Option[String],
      settings: Map[String, String] = Map.empty,
      username: String = PROFILE_USER): Unit = {
    withConnection(settings, username) { connection =>
      assert(queryValue(connection, "SELECT 1") === "1")
      assert(queryValue(connection, s"SET $MARKER", 2) === expected.getOrElse("legacy"))
      assert(queryValue(connection, s"SET $KYUUBI_ENGINE_PROFILE_NAME_KEY", 2) ===
        expected.getOrElse("<undefined>"))
    }
  }

  protected def assertRejected(
      settings: Map[String, String],
      expectedMessage: String,
      username: String = PROFILE_USER): Unit = {
    val sessionCount = server.backendService.sessionManager.getActiveUserSessionCount
    val error = intercept[SQLException] {
      val connection = openConnection(settings, username)
      connection.close()
    }
    assert(error.getMessage.contains(expectedMessage), error.getMessage)
    assert(server.backendService.sessionManager.getActiveUserSessionCount === sessionCount)
  }

  protected def assertBlacklisted(profile: String, extra: Map[String, String] = Map.empty): Unit = {
    assertRejected(
      extra + (ENGINE_PROFILE.key -> profile),
      s"Current user '$PROFILE_USER' is not allowed to use the engine profile '$profile'")
  }
}

class EngineProfileJdbcSuite extends EngineProfileJdbcTest {
  import EngineProfileJdbcTest._

  test("ADH-8475: JDBC explicit profile wins over user, group and engine-type defaults") {
    withServerConf(
      principalKey(PROFILE_USER, ENGINE_PROFILE.key) -> "beta",
      principalKey(FIRST_GROUP, ENGINE_PROFILE.key) -> "gamma",
      TYPE_DEFAULT -> "gamma") {
      assertProfile(Some("alpha"), Map(ENGINE_PROFILE.key -> "alpha"))
    }
  }

  test("ADH-8475: JDBC user and ordered group defaults precede the engine-type default") {
    withServerConf(
      principalKey(PROFILE_USER, ENGINE_PROFILE.key) -> "alpha",
      principalKey(FIRST_GROUP, ENGINE_PROFILE.key) -> "beta",
      principalKey(SECOND_GROUP, ENGINE_PROFILE.key) -> "gamma",
      TYPE_DEFAULT -> "gamma") {
      assertProfile(Some("alpha"))
    }
    withServerConf(
      principalKey(FIRST_GROUP, ENGINE_PROFILE.key) -> "beta",
      principalKey(SECOND_GROUP, ENGINE_PROFILE.key) -> "gamma",
      TYPE_DEFAULT -> "alpha") {
      assertProfile(Some("beta"))
    }
  }

  test("ADH-8475: JDBC engine-type defaults use request, user, group and server precedence") {
    withServerConf(
      ENGINE_TYPE.key -> "JDBC",
      principalKey(PROFILE_USER, ENGINE_TYPE.key) -> "TRINO",
      TYPE_DEFAULT -> "alpha") {
      assertProfile(Some("alpha"), Map(ENGINE_TYPE.key -> "SPARK_SQL"))
    }
    withServerConf(
      ENGINE_TYPE.key -> "JDBC",
      principalKey(PROFILE_USER, ENGINE_TYPE.key) -> "SPARK_SQL",
      principalKey(FIRST_GROUP, ENGINE_TYPE.key) -> "TRINO",
      TYPE_DEFAULT -> "alpha") {
      assertProfile(Some("alpha"))
    }
    withServerConf(
      ENGINE_TYPE.key -> "JDBC",
      principalKey(FIRST_GROUP, ENGINE_TYPE.key) -> "SPARK_SQL",
      TYPE_DEFAULT -> "alpha") {
      assertProfile(Some("alpha"))
    }
    withServerConf(TYPE_DEFAULT -> "alpha") {
      assertProfile(Some("alpha"))
    }
  }

  test("ADH-8475: JDBC sessions without any profile preserve the legacy engine") {
    assertProfile(None)
  }

  test("ADH-8475: JDBC engine type overrides the type declared by the selected profile") {
    assertProfile(
      Some("declared_trino"),
      Map(ENGINE_PROFILE.key -> "declared_trino", ENGINE_TYPE.key -> "SPARK_SQL"))
  }

  test("ADH-8475: JDBC exposes profile defaults and client settings override them") {
    withConnection(Map(ENGINE_PROFILE.key -> "alpha")) { connection =>
      assert(queryValue(connection, s"SET $MARKER", 2) === "alpha")
      assert(queryValue(connection, "SET kyuubi.session.idle.timeout", 2) === "PT2M")
      val declaration = s"kyuubi.engine.profile.beta.conf.$MARKER"
      assert(queryValue(connection, s"SET $declaration", 2) === "<undefined>")
      assert(queryValue(connection, s"SET spark.$declaration", 2) === "<undefined>")
    }
    withConnection(Map(
      ENGINE_PROFILE.key -> "alpha",
      MARKER -> "client",
      KYUUBI_ENGINE_PROFILE_NAME_KEY -> "beta")) { connection =>
      assert(queryValue(connection, s"SET $MARKER", 2) === "client")
      assert(queryValue(connection, s"SET $KYUUBI_ENGINE_PROFILE_NAME_KEY", 2) === "alpha")
    }
  }

  test("ADH-8475: JDBC ignore and restrict policies also apply with an engine profile") {
    withConnection(Map(ENGINE_PROFILE.key -> "alpha", IGNORED_MARKER -> "client")) { connection =>
      assert(queryValue(connection, s"SET $IGNORED_MARKER", 2) === "alpha")
    }
    assertRejected(
      Map(ENGINE_PROFILE.key -> "alpha", RESTRICTED_KEY -> "2"),
      s"$RESTRICTED_KEY is a restrict key according to the server-side configuration")
  }

  test("ADH-8475: JDBC unknown profiles fail with the profile resolution error") {
    withServerConf(TYPE_DEFAULT -> "alpha") {
      assertRejected(
        Map(ENGINE_PROFILE.key -> "missing"),
        "Engine profile 'missing' is not defined. " +
          "Available profiles: [alpha, beta, declared_trino, gamma].")
    }
  }

  test("ADH-9238: JDBC rejects explicit profiles blacklisted by the user or any group") {
    withServerConf(principalKey(PROFILE_USER, ENGINE_PROFILES_BLACKLIST.key) -> " alpha ") {
      assertBlacklisted("alpha")
    }
    withServerConf(principalKey(FIRST_GROUP, ENGINE_PROFILES_BLACKLIST.key) -> "beta") {
      assertBlacklisted("beta")
    }
    withServerConf(
      principalKey(PROFILE_USER, ENGINE_PROFILES_BLACKLIST.key) -> " alpha, beta ",
      principalKey(FIRST_GROUP, ENGINE_PROFILES_BLACKLIST.key) -> " beta ",
      principalKey(SECOND_GROUP, ENGINE_PROFILES_BLACKLIST.key) -> " gamma ") {
      Seq("alpha", "beta", "gamma").foreach(assertBlacklisted(_))
      assertProfile(Some("alpha"), Map(ENGINE_PROFILE.key -> "alpha"), OTHER_USER)
    }
  }

  test("ADH-9238: JDBC skips a blacklisted user default and uses an allowed group default") {
    withServerConf(
      principalKey(PROFILE_USER, ENGINE_PROFILE.key) -> "alpha",
      principalKey(FIRST_GROUP, ENGINE_PROFILE.key) -> "beta",
      principalKey(SECOND_GROUP, ENGINE_PROFILES_BLACKLIST.key) -> "alpha",
      TYPE_DEFAULT -> "gamma") {
      assertProfile(Some("beta"))
    }
  }

  test("ADH-9238: JDBC skips a blacklisted first group and uses the next allowed group") {
    withServerConf(
      principalKey(FIRST_GROUP, ENGINE_PROFILE.key) -> "alpha",
      principalKey(SECOND_GROUP, ENGINE_PROFILE.key) -> "beta",
      principalKey(PROFILE_USER, ENGINE_PROFILES_BLACKLIST.key) -> "alpha",
      TYPE_DEFAULT -> "gamma") {
      assertProfile(Some("beta"))
    }
  }

  test("ADH-9238: JDBC skips forbidden principal defaults and uses the engine-type default") {
    withServerConf(
      principalKey(PROFILE_USER, ENGINE_PROFILE.key) -> "alpha",
      principalKey(FIRST_GROUP, ENGINE_PROFILE.key) -> "beta",
      principalKey(PROFILE_USER, ENGINE_PROFILES_BLACKLIST.key) -> "alpha,beta",
      TYPE_DEFAULT -> "gamma") {
      assertProfile(Some("gamma"))
    }
  }

  test("ADH-9238: JDBC falls back to no profile when every candidate is blacklisted") {
    withServerConf(
      principalKey(PROFILE_USER, ENGINE_PROFILE.key) -> "alpha",
      principalKey(FIRST_GROUP, ENGINE_PROFILE.key) -> "beta",
      principalKey(SECOND_GROUP, ENGINE_PROFILE.key) -> "gamma",
      principalKey(PROFILE_USER, ENGINE_PROFILES_BLACKLIST.key) -> "alpha,beta",
      principalKey(SECOND_GROUP, ENGINE_PROFILES_BLACKLIST.key) -> "gamma",
      TYPE_DEFAULT -> "gamma") {
      assertProfile(None)
    }
  }

  test("ADH-9238: JDBC client settings cannot remove a server-side blacklist") {
    withServerConf(principalKey(FIRST_GROUP, ENGINE_PROFILES_BLACKLIST.key) -> "alpha") {
      assertBlacklisted(
        "alpha",
        Map(
          ENGINE_PROFILES_BLACKLIST.key -> "",
          principalKey(FIRST_GROUP, ENGINE_PROFILES_BLACKLIST.key) -> "",
          ENGINE_PROFILES_UNKNOWN_STRATEGY.key -> "LOG"))
    }
    withServerConf(ENGINE_PROFILES_BLACKLIST.key -> "alpha") {
      assertProfile(Some("alpha"), Map(ENGINE_PROFILE.key -> "alpha"))
    }
  }

  test("ADH-8475: live JDBC sessions reuse one profile and isolate different or absent profiles") {
    withConnection(Map(ENGINE_PROFILE.key -> "alpha")) { alpha =>
      val alphaId = queryValue(alpha, "SELECT engine_id()")
      assert(alphaId.nonEmpty)
      withConnection(Map(ENGINE_PROFILE.key -> "alpha")) { reused =>
        assert(queryValue(reused, "SELECT engine_id()") === alphaId)
        withConnection(Map(ENGINE_PROFILE.key -> "beta")) { beta =>
          val betaId = queryValue(beta, "SELECT engine_id()")
          assert(betaId !== alphaId)
          withConnection() { legacy =>
            val legacyId = queryValue(legacy, "SELECT engine_id()")
            assert(legacyId !== alphaId)
            assert(legacyId !== betaId)
            assert(queryValue(alpha, s"SET $MARKER", 2) === "alpha")
            assert(queryValue(beta, s"SET $MARKER", 2) === "beta")
            assert(queryValue(legacy, s"SET $MARKER", 2) === "legacy")
          }
        }
      }
    }
  }
}
