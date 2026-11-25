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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_TRANSPORT_SSL
import org.apache.kyuubi.session.SessionManager

class EngineSslPolicySuite extends KyuubiFunSuite {

  private val SPARK_CONNECT_SSL_ENABLED = "spark.ssl.connect.enabled"

  private val allEntries = Seq(
    ENGINE_SSL_ENABLED,
    ENGINE_SSL_KEYSTORE_PATH,
    ENGINE_SSL_KEYSTORE_PASSWORD,
    ENGINE_SSL_KEYSTORE_TYPE,
    ENGINE_SSL_KEYSTORE_ALGORITHM,
    ENGINE_SSL_DISALLOWED_PROTOCOLS,
    ENGINE_SSL_INCLUDE_CIPHER_SUITES,
    ENGINE_SSL_TRUSTSTORE_PATH,
    ENGINE_SSL_TRUSTSTORE_PASSWORD,
    ENGINE_SSL_TRUSTSTORE_TYPE,
    ENGINE_SSL_EXPECTED_HOSTNAME)

  private val serverValues = Map(
    ENGINE_SSL_ENABLED.key -> "true",
    ENGINE_SSL_KEYSTORE_PATH.key -> "/etc/security/engine.p12",
    ENGINE_SSL_KEYSTORE_PASSWORD.key -> "server-literal-secret",
    ENGINE_SSL_KEYSTORE_TYPE.key -> "PKCS12",
    ENGINE_SSL_KEYSTORE_ALGORITHM.key -> "PKIX",
    ENGINE_SSL_DISALLOWED_PROTOCOLS.key -> "SSLv3,TLSv1,TLSv1.1,TLSv1.2",
    ENGINE_SSL_INCLUDE_CIPHER_SUITES.key -> "TLS_AES_128_GCM_SHA256",
    ENGINE_SSL_TRUSTSTORE_PATH.key -> "/etc/security/truststore.p12",
    ENGINE_SSL_TRUSTSTORE_PASSWORD.key -> "truststore-secret",
    ENGINE_SSL_TRUSTSTORE_TYPE.key -> "PKCS12",
    ENGINE_SSL_EXPECTED_HOSTNAME.key -> "engine.example.com")

  private def engineSslSettings(kyuubiConf: KyuubiConf): Map[String, String] =
    kyuubiConf.getAll.filter { case (key, _) => EngineSslPolicy.isEngineSslKey(key) }

  private def conf(settings: (String, String)*): KyuubiConf = {
    val kyuubiConf = KyuubiConf(loadSysDefault = false)
    settings.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  // what an engine profile or a conf advisor overlay may put into the session conf
  private def sessionConfWithOverrides(engineType: String): KyuubiConf = conf(
    ENGINE_TYPE.key -> engineType,
    ENGINE_SSL_ENABLED.key -> "false",
    ENGINE_SSL_KEYSTORE_PATH.key -> "/tmp/attacker.p12",
    ENGINE_SSL_TRUSTSTORE_PATH.key -> "/tmp/attacker-truststore.p12",
    "spark.kyuubi.engine.ssl.enabled" -> "false",
    "spark.kyuubi.engine.ssl.keystore.password" -> "profile-secret",
    // no engine TLS key, engines never serve Spark Connect TLS from it
    SPARK_CONNECT_SSL_ENABLED -> "true",
    ENGINE_SHARE_LEVEL.key -> "USER")

  test("engine TLS is only supported by Spark SQL engines") {
    assert(EngineSslPolicy.supports("SPARK_SQL"))
    assert(EngineSslPolicy.supports("spark_sql"))
    Seq("FLINK_SQL", "HIVE_SQL", "JDBC", "TRINO", "CHAT", "UNKNOWN", "").foreach { engineType =>
      assert(!EngineSslPolicy.supports(engineType), engineType)
    }
    assert(EngineSslPolicy.SUPPORTED_ENGINE_TYPES === Set(EngineType.SPARK_SQL))
  }

  test("engine TLS keys") {
    allEntries.foreach { entry =>
      assert(EngineSslPolicy.isEngineSslKey(entry.key))
      assert(EngineSslPolicy.isEngineSslKey(s"spark.${entry.key}"))
    }
    Seq(
      ENGINE_SECURITY_ENABLED.key,
      ENGINE_SHARE_LEVEL.key,
      KYUUBI_ENGINE_TRANSPORT_SSL,
      FRONTEND_SSL_KEYSTORE_PATH.key,
      "spark.ssl.enabled",
      "spark.ssl.keyStore",
      "spark.ssl.ui.enabled",
      SPARK_CONNECT_SSL_ENABLED,
      "spark.ssl.connect.keyStore").foreach { key =>
      assert(!EngineSslPolicy.isEngineSslKey(key), key)
    }
  }

  test("every engine TLS key is always restricted for clients") {
    val restricted = SessionManager.ALWAYS_RESTRICTED_CONF_PREFIXES
    EngineSslPolicy.PREFIXES.foreach { prefix =>
      assert(restricted.contains(s"$prefix*"), prefix)
    }
    val restrictedPrefixes = restricted.filter(_.endsWith(".*")).map(_.stripSuffix("*"))
    allEntries.map(_.key).flatMap(key => Seq(key, s"spark.$key")).foreach { key =>
      assert(restrictedPrefixes.exists(key.startsWith), key)
    }
  }

  test("applyTo strips every engine TLS key and forwards the server allowlist") {
    val serverConf = conf(serverValues.toSeq: _*)
    val sessionConf = sessionConfWithOverrides("SPARK_SQL")

    val (enabled, stripped) = EngineSslPolicy.applyTo(serverConf, sessionConf)

    assert(enabled)
    assert(stripped === Seq(
      ENGINE_SSL_ENABLED.key,
      ENGINE_SSL_KEYSTORE_PATH.key,
      ENGINE_SSL_TRUSTSTORE_PATH.key,
      "spark.kyuubi.engine.ssl.enabled",
      "spark.kyuubi.engine.ssl.keystore.password"))
    assert(engineSslSettings(sessionConf) === Map(
      ENGINE_SSL_ENABLED.key -> "true",
      ENGINE_SSL_KEYSTORE_PATH.key -> "/etc/security/engine.p12",
      ENGINE_SSL_KEYSTORE_TYPE.key -> "PKCS12",
      ENGINE_SSL_KEYSTORE_ALGORITHM.key -> "PKIX",
      ENGINE_SSL_DISALLOWED_PROTOCOLS.key -> "SSLv3,TLSv1,TLSv1.1,TLSv1.2",
      ENGINE_SSL_INCLUDE_CIPHER_SUITES.key -> "TLS_AES_128_GCM_SHA256"))
    assert(sessionConf.get(ENGINE_SHARE_LEVEL) === "USER")
    assert(sessionConf.getOption(SPARK_CONNECT_SSL_ENABLED) === Some("true"))
    // no secret, trust store or expected host name ever reaches the engine
    assert(!sessionConf.getAll.values.exists(_.contains("secret")))
    assert(sessionConf.getOption(ENGINE_SSL_TRUSTSTORE_TYPE.key).isEmpty)
    assert(sessionConf.getOption(ENGINE_SSL_EXPECTED_HOSTNAME.key).isEmpty)
  }

  test("applyTo only forwards keys the server sets explicitly") {
    val serverConf = conf(
      ENGINE_SSL_ENABLED.key -> "true",
      ENGINE_SSL_KEYSTORE_PATH.key -> "/etc/security/engine.p12")
    val sessionConf = conf(ENGINE_TYPE.key -> "SPARK_SQL")

    assert(EngineSslPolicy.applyTo(serverConf, sessionConf) === ((true, Nil)))
    assert(engineSslSettings(sessionConf) === Map(
      ENGINE_SSL_ENABLED.key -> "true",
      ENGINE_SSL_KEYSTORE_PATH.key -> "/etc/security/engine.p12"))
  }

  test("applyTo with TLS off forwards only the decision to Spark SQL engines") {
    val off = conf(serverValues.toSeq: _*).set(ENGINE_SSL_ENABLED, false)
    val sessionConf = sessionConfWithOverrides("SPARK_SQL")

    val (enabled, stripped) = EngineSslPolicy.applyTo(off, sessionConf)

    assert(!enabled)
    assert(stripped.size === 5)
    // set explicitly, so that the engine-side spark-defaults.conf cannot turn TLS on
    assert(engineSslSettings(sessionConf) === Map(ENGINE_SSL_ENABLED.key -> "false"))
    assert(sessionConf.get(ENGINE_TYPE) === "SPARK_SQL")
  }

  test("applyTo only strips for engine types without engine TLS") {
    val on = conf(serverValues.toSeq: _*)
    Seq("FLINK_SQL", "JDBC", "NOT_AN_ENGINE_TYPE").foreach { engineType =>
      val sessionConf = sessionConfWithOverrides(engineType)
      val (enabled, stripped) = EngineSslPolicy.applyTo(on, sessionConf)
      assert(!enabled, engineType)
      assert(stripped.size === 5, engineType)
      assert(!sessionConf.getAll.keys.exists(EngineSslPolicy.isEngineSslKey), engineType)
      assert(sessionConf.getOption(ENGINE_TYPE.key) === Some(engineType))
    }
  }

  test("withoutEngineSslKeys") {
    val engineConf = Map(
      ENGINE_SSL_ENABLED.key -> "false",
      "spark.kyuubi.engine.ssl.keystore.path" -> "/tmp/x.p12",
      ENGINE_SHARE_LEVEL.key -> "USER",
      "spark.executor.memory" -> "2g")
    assert(EngineSslPolicy.withoutEngineSslKeys(engineConf) === Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      "spark.executor.memory" -> "2g"))
  }

  test("diagnoseMismatch") {
    val tls = Map(KYUUBI_ENGINE_TRANSPORT_SSL -> "true", "kyuubi.engine.id" -> "app-1")
    val plain = Map("kyuubi.engine.id" -> "app-1")
    val hostPort = "engine-host:18000"

    assert(EngineSslPolicy.diagnoseMismatch(expectedTls = true, Some(tls), hostPort).isEmpty)
    assert(EngineSslPolicy.diagnoseMismatch(expectedTls = false, Some(plain), hostPort).isEmpty)
    assert(EngineSslPolicy.diagnoseMismatch(expectedTls = true, None, hostPort).isEmpty)
    assert(EngineSslPolicy.diagnoseMismatch(expectedTls = false, None, hostPort).isEmpty)

    val expectTls = EngineSslPolicy.diagnoseMismatch(expectedTls = true, Some(plain), hostPort)
    assert(expectTls.exists(_.contains(s"Engine $hostPort was started with TLS off")))
    assert(expectTls.exists(_.contains(s"${ENGINE_SSL_ENABLED.key}=true")))
    assert(expectTls.exists(_.contains("deregistering")))

    val expectPlain = EngineSslPolicy.diagnoseMismatch(expectedTls = false, Some(tls), hostPort)
    assert(expectPlain.exists(_.contains(s"Engine $hostPort was started with TLS on")))
    assert(expectPlain.exists(_.contains(s"${ENGINE_SSL_ENABLED.key}=false")))
  }

  test("startupWarnings name keys and never values") {
    val literal = "server-literal-secret"
    val warnings = EngineSslPolicy.startupWarnings(conf(
      ENGINE_SSL_ENABLED.key -> "true",
      ENGINE_SSL_KEYSTORE_PASSWORD.key -> literal,
      s"spark.${ENGINE_SSL_KEYSTORE_PASSWORD.key}" -> literal))
    assert(warnings.size === 4)
    assert(warnings.exists(_.startsWith(s"${ENGINE_SSL_KEYSTORE_PASSWORD.key} is set")))
    assert(warnings.exists(_.startsWith(s"spark.${ENGINE_SSL_KEYSTORE_PASSWORD.key} is set")))
    assert(warnings.exists(_.contains(ENGINE_SSL_TRUSTSTORE_PATH.key)))
    assert(warnings.exists(_.contains(s"${ENGINE_SECURITY_ENABLED.key} is false")))
    assert(!warnings.exists(_.contains(literal)))

    val clean = conf(
      ENGINE_SSL_ENABLED.key -> "true",
      ENGINE_SSL_TRUSTSTORE_PATH.key -> "/etc/security/truststore.p12",
      ENGINE_SECURITY_ENABLED.key -> "true")
    assert(EngineSslPolicy.startupWarnings(clean).isEmpty)
  }
}
