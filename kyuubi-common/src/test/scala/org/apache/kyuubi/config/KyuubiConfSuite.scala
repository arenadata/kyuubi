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

package org.apache.kyuubi.config

import java.time.Duration

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.EngineSslPolicy
import org.apache.kyuubi.session.SessionManager

class KyuubiConfSuite extends KyuubiFunSuite {

  import KyuubiConf._

  test("kyuubi conf defaults") {
    val conf = new KyuubiConf()
    assert(conf.get(SERVER_PRINCIPAL) === None)
    assert(conf.get(KINIT_MAX_ATTEMPTS) === 10)
    assert(conf.get(OPERATION_IDLE_TIMEOUT) === Duration.ofHours(3).toMillis)
  }

  test("Flight SQL frontend configuration") {
    val conf = KyuubiConf()
    assert(conf.get(FRONTEND_FLIGHT_SQL_BIND_PORT) === 10299)
    assert(conf.get(FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS) === 1000)
    assert(conf.get(FRONTEND_FLIGHT_SQL_SSL_ENABLED) === false)
    assert(conf.get(FRONTEND_FLIGHT_SQL_SSL_CERT_FILE).isEmpty)
    assert(conf.get(FRONTEND_FLIGHT_SQL_SSL_KEY_FILE).isEmpty)
    assert(conf.isFlightSqlEnabled === false)

    conf.set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
    assert(conf.isFlightSqlEnabled)

    conf.set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)
    assert(conf.get(FRONTEND_FLIGHT_SQL_BIND_PORT) === 0)
    conf.set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
    assert(conf.get(FRONTEND_FLIGHT_SQL_BIND_HOST).contains("localhost"))

    assertThrows[IllegalArgumentException](
      conf.set(FRONTEND_FLIGHT_SQL_BIND_PORT, 1024).get(FRONTEND_FLIGHT_SQL_BIND_PORT))
    assertThrows[IllegalArgumentException](
      conf.set(FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS, 0).get(FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS))
  }

  test("kyuubi conf w/ w/o no sys defaults") {
    val key = "kyuubi.conf.abc"
    System.setProperty(key, "xyz")
    assert(KyuubiConf(false).getOption(key).isEmpty)
    assert(KyuubiConf(true).getAll.contains(key))
  }

  test("load default config file") {
    val conf = KyuubiConf().loadFileDefaults()
    assert(conf.getOption("kyuubi.yes").get === "yes")
    assert(conf.getOption("spark.kyuubi.yes").get === "no")
  }

  test("load config from --conf arguments") {
    val conf = KyuubiConf()
    assert(conf.get(KINIT_MAX_ATTEMPTS) === 10)

    val args: Array[String] = Array("--conf", "kyuubi.kinit.max.attempts=15")
    conf.loadFromArgs(args)
    assert(conf.get(KINIT_MAX_ATTEMPTS) === 15)
  }

  test("set and unset conf") {
    val conf = new KyuubiConf(false)

    val key = "kyuubi.conf.abc"
    conf.set(key, "opq")
    assert(conf.getOption(key) === Some("opq"))

    conf.set(OPERATION_IDLE_TIMEOUT, 5L)
    assert(conf.get(OPERATION_IDLE_TIMEOUT) === 5)

    conf.set(FRONTEND_THRIFT_BINARY_BIND_HOST.key, "kentyao.org")
    assert(conf.get(FRONTEND_THRIFT_BINARY_BIND_HOST).get === "kentyao.org")

    conf.set(FRONTEND_THRIFT_HTTP_BIND_HOST.key, "kentyao.org")
    assert(conf.get(FRONTEND_THRIFT_HTTP_BIND_HOST).get === "kentyao.org")

    conf.setIfMissing(OPERATION_IDLE_TIMEOUT, 60L)
    assert(conf.get(OPERATION_IDLE_TIMEOUT) === 5)

    conf.setIfMissing(FRONTEND_THRIFT_MIN_WORKER_THREADS, 2188)
    assert(conf.get(FRONTEND_THRIFT_MIN_WORKER_THREADS) === 2188)

    conf.unset(FRONTEND_THRIFT_MIN_WORKER_THREADS)
    assert(conf.get(FRONTEND_THRIFT_MIN_WORKER_THREADS) === 9)

    conf.unset(key)
    assert(conf.getOption(key).isEmpty)

    val map = conf.getAllWithPrefix("kyuubi", "")
    assert(map(FRONTEND_THRIFT_BINARY_BIND_HOST.key.substring(7)) === "kentyao.org")
    assert(map(FRONTEND_THRIFT_HTTP_BIND_HOST.key.substring(7)) === "kentyao.org")
    val map1 = conf.getAllWithPrefix("kyuubi", "operation")
    assert(map1(OPERATION_IDLE_TIMEOUT.key.substring(7)) === "PT0.005S")
    assert(map1.size === 1)
  }

  test("clone") {
    val conf = KyuubiConf()
    val key = "kyuubi.abc.conf"
    conf.set(key, "xyz")
    val cloned = conf.clone
    assert(conf !== cloned)
    assert(cloned.getOption(key).get === "xyz")
  }

  test("get user specific defaults") {
    val conf = KyuubiConf().loadFileDefaults()

    assert(conf.getUserDefaults("kyuubi").getOption("spark.user.test").get === "a")
    assert(conf.getUserDefaults("userb").getOption("spark.user.test").get === "b")
    assert(conf.getUserDefaults("userc").getOption("spark.user.test").get === "c")
  }

  test("support arbitrary config from kyuubi-defaults") {
    val conf = KyuubiConf()
    assert(conf.getOption("user.name").isEmpty)
    conf.loadFileDefaults()
    assert(conf.getOption("abc").get === "xyz")
    assert(conf.getOption("xyz").get === "abc")
  }

  test("time config test") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT, 1000L)
    assert(kyuubiConf.get(OPERATION_IDLE_TIMEOUT) === 1000L)
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT.key, "1000")
    assert(kyuubiConf.get(OPERATION_IDLE_TIMEOUT) === 1000L)
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT.key, "  1000  ")
    assert(kyuubiConf.get(OPERATION_IDLE_TIMEOUT) === 1000L)
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT.key, "1000A")
    val e = intercept[IllegalArgumentException](kyuubiConf.get(OPERATION_IDLE_TIMEOUT))
    assert(e.getMessage.contains("ISO-8601"))
    kyuubiConf.set(OPERATION_IDLE_TIMEOUT.key, "  P1DT2H3.2S  ")

    assert(kyuubiConf.get(OPERATION_IDLE_TIMEOUT) ===
      Duration.ofDays(1)
        .plusHours(2)
        .plusSeconds(3)
        .plusMillis(200)
        .toMillis)
  }

  test(KyuubiConf.OPERATION_QUERY_TIMEOUT.key) {
    val kyuubiConf = KyuubiConf()
    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT).isEmpty)
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT, 1000L)
    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT) === Some(1000L))
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "1000")
    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT) === Some(1000L))
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "  1000  ")
    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT) === Some(1000L))
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "1000A")
    val e = intercept[IllegalArgumentException](kyuubiConf.get(OPERATION_QUERY_TIMEOUT))
    assert(e.getMessage.contains("ISO-8601"))
    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "  P1DT2H3.2S  ")

    assert(kyuubiConf.get(OPERATION_QUERY_TIMEOUT) ===
      Some(Duration.ofDays(1)
        .plusHours(2)
        .plusSeconds(3)
        .plusMillis(200)
        .toMillis))

    kyuubiConf.set(OPERATION_QUERY_TIMEOUT.key, "0")
    val e1 = intercept[IllegalArgumentException](kyuubiConf.get(OPERATION_QUERY_TIMEOUT))
    assert(e1.getMessage.contains("must >= 1s if set"))
  }

  test("kyuubi conf engine.share.level.subdomain valid path test") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "")
    assertThrows[IllegalArgumentException](kyuubiConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN))
    kyuubiConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, ".")
    assertThrows[IllegalArgumentException](kyuubiConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN))
    kyuubiConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "..")
    assertThrows[IllegalArgumentException](kyuubiConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN))
    kyuubiConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "/")
    assertThrows[IllegalArgumentException](kyuubiConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN))
    kyuubiConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "/tmp/")
    assertThrows[IllegalArgumentException](kyuubiConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN))
    kyuubiConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "tmp/")
    assertThrows[IllegalArgumentException](kyuubiConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN))
    kyuubiConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "/tmp")
    assertThrows[IllegalArgumentException](kyuubiConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN))
    kyuubiConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, "abc/efg")
    assertThrows[IllegalArgumentException](kyuubiConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN))
    val path = "kyuubi!@#$%^&*()_+-=[]{};:,.<>?"
    kyuubiConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN.key, path)
    assert(kyuubiConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN).get == path)
  }

  test("get pre-defined batch conf for different batch types") {
    val kyuubiConf = KyuubiConf()
    kyuubiConf.set(s"$KYUUBI_BATCH_CONF_PREFIX.spark.spark.yarn.tags", "kyuubi")
    kyuubiConf.set(s"$KYUUBI_BATCH_CONF_PREFIX.flink.yarn.tags", "kyuubi")
    assert(kyuubiConf.getBatchConf("spark") == Map("spark.yarn.tags" -> "kyuubi"))
    assert(kyuubiConf.getBatchConf("flink") == Map("yarn.tags" -> "kyuubi"))
  }

  test("KYUUBI #3848 - Sort config map returned in KyuubiConf.getAll") {
    val kyuubiConf = KyuubiConf(false)
    kyuubiConf.set("kyuubi.xyz", "123")
    kyuubiConf.set("kyuubi.efg", "")
    kyuubiConf.set("kyuubi.abc", "789")

    var kSeq = Seq[String]()
    kyuubiConf.getAll.foreach { case (k, v) =>
      kSeq = kSeq :+ k
    }

    assertResult(kSeq.size)(3)
    assertResult(kSeq.head)("kyuubi.abc")
    assertResult(kSeq(1))("kyuubi.efg")
    assertResult(kSeq(2))("kyuubi.xyz")
  }

  test("KYUUBI #4843 - Support multiple kubernetes contexts and namespaces") {
    val kyuubiConf = KyuubiConf(false)
    kyuubiConf.set("kyuubi.kubernetes.28.master.address", "k8s://master")
    kyuubiConf.set(
      "kyuubi.kubernetes.28.ns1.authenticate.oauthTokenFile",
      "/var/run/secrets/kubernetes.io/token.ns1")
    kyuubiConf.set(
      "kyuubi.kubernetes.28.ns2.authenticate.oauthTokenFile",
      "/var/run/secrets/kubernetes.io/token.ns2")

    val kubernetesConf1 = kyuubiConf.getKubernetesConf(Some("28"), Some("ns1"))
    assert(kubernetesConf1.get(KyuubiConf.KUBERNETES_MASTER) == Some("k8s://master"))
    assert(kubernetesConf1.get(KyuubiConf.KUBERNETES_AUTHENTICATE_OAUTH_TOKEN_FILE) ==
      Some("/var/run/secrets/kubernetes.io/token.ns1"))

    val kubernetesConf2 = kyuubiConf.getKubernetesConf(Some("28"), Some("ns2"))
    assert(kubernetesConf2.get(KyuubiConf.KUBERNETES_MASTER) == Some("k8s://master"))
    assert(kubernetesConf2.get(KyuubiConf.KUBERNETES_AUTHENTICATE_OAUTH_TOKEN_FILE) ==
      Some("/var/run/secrets/kubernetes.io/token.ns2"))
  }

  test("KYUUBI #7055 - Support to exclude server only configs with prefixes") {
    val kyuubiConf = KyuubiConf(false)
    kyuubiConf.set("kyuubi.backend.server.event.kafka.broker", "localhost:9092")
    assert(kyuubiConf.getUserDefaults("kyuubi").getAll.size == 0)
    assert(kyuubiConf.getUserDefaults("user").getAll.size == 0)
  }

  test("engine profile declarations are server-only and not passed to the engine") {
    val kyuubiConf = KyuubiConf(false)
    kyuubiConf.set("kyuubi.engine.profile.pg.type", "JDBC")
    kyuubiConf.set("kyuubi.engine.profile.pg.env.SECRET_TOKEN", "env-secret")
    kyuubiConf.set("kyuubi.engine.profile.pg.session.engine.trino.connection.url", "http://host")
    kyuubiConf.set(
      "kyuubi.engine.profile.pg.conf.kyuubi.engine.jdbc.connection.password",
      "conf-secret")

    // a plain server default that is not a profile declaration must still pass through
    kyuubiConf.set("spark.some.tuning", "1g")

    val userConf = kyuubiConf.getUserDefaults("user")

    assert(userConf.getOption("spark.some.tuning").contains("1g"))
    assert(!userConf.getAll.keys.exists(_.startsWith("kyuubi.engine.profile.")))
    assert(!userConf.getAll.values.exists(_.contains("env-secret")))
    assert(!userConf.getAll.values.exists(_.contains("conf-secret")))

    // check that the profile-name attribute (published for discovery) still flows through
    assert(kyuubiConf.getOption(KyuubiReservedKeys.KYUUBI_ENGINE_PROFILE_NAME_KEY).isEmpty)
    kyuubiConf.set(KyuubiReservedKeys.KYUUBI_ENGINE_PROFILE_NAME_KEY, "pg")
    assert(kyuubiConf.getUserDefaults("kyuubi")
      .getOption(KyuubiReservedKeys.KYUUBI_ENGINE_PROFILE_NAME_KEY).contains("pg"))
  }

  private val engineSslEntries: Seq[ConfigEntry[_]] = Seq(
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

  test("engine TLS configuration defaults") {
    val conf = KyuubiConf(false)
    assert(conf.get(ENGINE_SSL_ENABLED) === false)
    assert(conf.get(ENGINE_SSL_KEYSTORE_PATH).isEmpty)
    assert(conf.get(ENGINE_SSL_KEYSTORE_PASSWORD).isEmpty)
    assert(conf.get(ENGINE_SSL_KEYSTORE_TYPE).isEmpty)
    assert(conf.get(ENGINE_SSL_KEYSTORE_ALGORITHM).isEmpty)
    assert(conf.get(ENGINE_SSL_DISALLOWED_PROTOCOLS) === Set("SSLv2", "SSLv3", "TLSv1", "TLSv1.1"))
    assert(conf.get(ENGINE_SSL_INCLUDE_CIPHER_SUITES).isEmpty)
    assert(conf.get(ENGINE_SSL_TRUSTSTORE_PATH).isEmpty)
    assert(conf.get(ENGINE_SSL_TRUSTSTORE_PASSWORD).isEmpty)
    assert(conf.get(ENGINE_SSL_TRUSTSTORE_TYPE).isEmpty)
    assert(conf.get(ENGINE_SSL_EXPECTED_HOSTNAME).isEmpty)

    conf.set(ENGINE_SSL_EXPECTED_HOSTNAME.key, "engine.example.com")
    assert(conf.get(ENGINE_SSL_EXPECTED_HOSTNAME) === Some("engine.example.com"))
    conf.set(ENGINE_SSL_EXPECTED_HOSTNAME.key, "localhost")
    assert(conf.get(ENGINE_SSL_EXPECTED_HOSTNAME) === Some("localhost"))
    // a port or a space would make every Thrift handshake fail, while gRPC would ignore them
    val invalidHostnames =
      Seq("", " ", "*.example.com", "engine.example.com:10009", "engine example.com")
    invalidHostnames.foreach { invalid =>
      conf.set(ENGINE_SSL_EXPECTED_HOSTNAME.key, invalid)
      intercept[IllegalArgumentException](conf.get(ENGINE_SSL_EXPECTED_HOSTNAME))
    }
  }

  test("engine TLS entries are server only and stripped from user defaults") {
    engineSslEntries.foreach { entry =>
      assert(entry.serverOnly, entry.key)
      assert(!entry.internal, entry.key)
      assert(entry.version === "1.11.1", entry.key)
    }

    val conf = KyuubiConf(false)
    engineSslEntries.foreach(entry => conf.set(entry.key, "server-value"))
    conf.set(s"___user___.${ENGINE_SSL_ENABLED.key}", "false")
    conf.set(s"___user___.${ENGINE_SSL_KEYSTORE_PATH.key}", "/tmp/user.p12")
    Seq("kyuubi", "user").foreach { user =>
      val userDefaults = conf.getUserDefaults(user)
      assert(!userDefaults.getAll.keys.exists(_.startsWith("kyuubi.engine.ssl.")), user)
    }
  }

  test("every registered engine TLS entry is restricted for clients") {
    val registered = KyuubiConf.getConfigEntries().asScala
      .filter(_.key.startsWith("kyuubi.engine.ssl."))
      .map(_.key).toSet
    assert(registered === engineSslEntries.map(_.key).toSet)
    val restrictedPrefixes = SessionManager.ALWAYS_RESTRICTED_CONF_PREFIXES
      .filter(_.endsWith(".*")).map(_.stripSuffix("*"))
    registered.flatMap(key => Seq(key, s"spark.$key")).foreach { key =>
      assert(restrictedPrefixes.exists(key.startsWith), key)
      assert(EngineSslPolicy.isEngineSslKey(key), key)
    }
  }
}
