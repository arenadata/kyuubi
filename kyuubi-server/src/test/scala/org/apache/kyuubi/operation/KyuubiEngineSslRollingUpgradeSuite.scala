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

package org.apache.kyuubi.operation

import java.io.IOException
import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.sql.{Connection, SQLException}
import java.util.Properties

import scala.util.control.NonFatal

import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.logging.log4j.Level
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_TRANSPORT_SSL
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_NAMESPACE, HA_ZK_AUTH_TYPE}
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.jdbc.KyuubiHiveDriver
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.session.KyuubiSessionImpl
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

/**
 * Kyuubi servers with different `kyuubi.engine.ssl.enabled` sharing one engine space, as during
 * a rolling upgrade: an engine started with the other setting is detected through its discovery
 * attribute, deregistered and replaced, whatever `kyuubi.session.engine.open.onFailure` says.
 * The engines of the server without engine TLS read a spark-defaults.conf that turns it on, the
 * server's own decision must win over it.
 */
class KyuubiEngineSslRollingUpgradeSuite extends KyuubiFunSuite {
  import KyuubiOperationWithEngineSslSuite._

  private val namespace = "kyuubi_engine_ssl_rolling_upgrade"

  private var zkServer: EmbeddedZookeeper = _
  private var plainServer: KyuubiServer = _
  private var tlsServer: KyuubiServer = _
  // gives up at the first failed attempt to open an engine session
  private var noRetryTlsServer: KyuubiServer = _

  private lazy val credentialProviderPath = createCredentialProvider(
    materialDir,
    ENGINE_SSL_KEYSTORE_PASSWORD.key -> passwordOf(goodKeyStore))

  // what an operator may leave in the engine-side spark-defaults.conf
  private lazy val tlsSparkConfDir: Path = {
    val dir = Utils.createTempDir("engine-ssl-spark-conf")
    val defaults = Seq(
      s"spark.${ENGINE_SSL_ENABLED.key}" -> "true",
      s"spark.${ENGINE_SSL_KEYSTORE_PATH.key}" -> goodKeyStore.toString,
      s"spark.${ENGINE_SSL_KEYSTORE_TYPE.key}" -> storeTypeOf(goodKeyStore),
      s"spark.${ENGINE_SSL_KEYSTORE_PASSWORD.key}" -> passwordOf(goodKeyStore))
    val content = defaults.map { case (k, v) => s"$k $v\n" }.mkString
    Files.write(dir.resolve("spark-defaults.conf"), content.getBytes(StandardCharsets.UTF_8))
    dir
  }

  private def serverConf(name: String, engineSsl: Boolean): KyuubiConf = {
    val conf = KyuubiConf()
      .set(SERVER_NAME, name)
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.THRIFT_BINARY.toString))
      .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
      .set(FRONTEND_REST_BIND_PORT, 0)
      .set(FRONTEND_MYSQL_BIND_PORT, 0)
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_ZK_AUTH_TYPE, AuthTypes.NONE.toString)
      .set(HA_NAMESPACE, namespace)
      .set(ENGINE_SHARE_LEVEL, "USER")
      .set(SESSION_ENGINE_LAUNCH_ASYNC, false)
      .set(ENGINE_OPEN_RETRY_WAIT, 1000L)
      .set(ENGINE_CHECK_INTERVAL, 1000L)
      // long enough for an idle engine to wait for the next server in a test
      .set(ENGINE_IDLE_TIMEOUT, 60000L)
      .set(ENGINE_SSL_ENABLED, engineSsl)
      .set("kyuubi.metrics.enabled", "false")
      .set("kyuubi.ha.zookeeper.connection.retry.policy", "ONE_TIME")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
    if (engineSsl) {
      // Engines register their real host, the servers verify their certificates against a
      // fixed name instead, which the test certificate covers on any machine.
      val settings = engineSslServerConf(goodKeyStore) ++ Map(
        ENGINE_SSL_EXPECTED_HOSTNAME.key -> "localhost",
        CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH -> credentialProviderPath)
      settings.foreach { case (k, v) => conf.set(k, v) }
    } else {
      conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.SPARK_CONF_DIR", tlsSparkConfDir.toString)
    }
    conf
  }

  override def beforeAll(): Unit = {
    zkServer = new EmbeddedZookeeper()
    val zkConf = KyuubiConf()
      .set(ZookeeperConf.ZK_CLIENT_PORT, 0)
      .set(ZookeeperConf.ZK_DATA_DIR, Utils.createTempDir().toString)
    zkServer.initialize(zkConf)
    zkServer.start()
    try {
      plainServer = KyuubiServer.startServer(serverConf("kyuubi-engine-ssl-off", false))
      tlsServer = KyuubiServer.startServer(serverConf("kyuubi-engine-ssl-on", true))
      noRetryTlsServer = KyuubiServer.startServer(
        serverConf("kyuubi-engine-ssl-on-no-retry", true).set(ENGINE_OPEN_MAX_ATTEMPTS, 0))
    } catch {
      case NonFatal(e) =>
        stopAll()
        throw e
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try stopAll()
    finally super.afterAll()
  }

  private def stopAll(): Unit = {
    Seq(noRetryTlsServer, tlsServer, plainServer).filter(_ != null).foreach { server =>
      sessionsOf(server).foreach { session =>
        try server.backendService.sessionManager.closeSession(session.handle)
        catch {
          case NonFatal(e) => warn(s"Failed to close session ${session.handle}", e)
        }
      }
      try server.stop()
      catch {
        case NonFatal(e) => warn(s"Failed to stop server ${server.getName}", e)
      }
    }
    noRetryTlsServer = null
    tlsServer = null
    plainServer = null
    if (zkServer != null) {
      zkServer.stop()
      zkServer = null
    }
  }

  private def connect(server: KyuubiServer): Connection = {
    val info = new Properties()
    info.setProperty("user", Utils.currentUser)
    info.setProperty("password", "anonymous")
    new KyuubiHiveDriver().connect(
      s"jdbc:hive2://${server.frontendServices.head.connectionUrl}/;",
      info)
  }

  /** An engine as it registered itself in service discovery. */
  private case class Engine(space: String, refId: String, host: String, port: Int, tls: Boolean)

  /** Runs a query through the server and returns the engine that served it. */
  private def engineThrough(server: KyuubiServer): Engine = {
    val connection = connect(server)
    try {
      val rs = connection.createStatement().executeQuery("SELECT 1")
      assert(rs.next())
      assert(rs.getInt(1) === 1)
      val sessions = sessionsOf(server)
      assert(sessions.size === 1)
      val session = sessions.head
      assert(session.engineSslEnabled === server.getConf.get(ENGINE_SSL_ENABLED))
      val node = session.getEngineNode.getOrElse(fail("the engine is not registered"))
      Engine(
        session.engine.engineSpace,
        node.engineRefId.getOrElse(fail(s"no engine ref id in $node")),
        node.host,
        node.port,
        node.attributes.get(KYUUBI_ENGINE_TRANSPORT_SSL).contains("true"))
    } finally {
      connection.close()
    }
  }

  private def assertDeregistered(engine: Engine): Unit = {
    val refIds = withDiscoveryClient(tlsServer.getConf) { client =>
      client.getServiceNodesInfo(engine.space, silent = true).flatMap(_.engineRefId)
    }
    assert(!refIds.contains(engine.refId))
  }

  private def acceptsConnections(host: String, port: Int): Boolean = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress(host, port), 5000)
      true
    } catch {
      case _: IOException => false
    } finally {
      socket.close()
    }
  }

  /** A deregistered engine without sessions stops, unless its replacement took its port. */
  private def assertStops(engine: Engine, replacement: Option[Engine]): Unit = {
    if (!replacement.exists(r => r.host == engine.host && r.port == engine.port)) {
      eventually(timeout(2.minutes), interval(1.second)) {
        assert(!acceptsConnections(engine.host, engine.port))
      }
    }
  }

  /** How a server with the other TLS setting explains that it replaces the engine. */
  private def mismatchMessage(engine: Engine): String =
    s"Engine ${engine.host}:${engine.port} was started with TLS " +
      s"${if (engine.tls) "on" else "off"} but this Kyuubi server has " +
      s"${ENGINE_SSL_ENABLED.key}=${!engine.tls}"

  private def mismatchWarnings(appender: LogAppender, engine: Engine): Seq[String] =
    appender.loggingEvents.filter(_.getLevel == Level.WARN)
      .map(_.getMessage.getFormattedMessage)
      .filter(_.startsWith(mismatchMessage(engine)))
      .toSeq

  test("a server with engine TLS replaces the plaintext engine of a server without it") {
    val plainEngine = engineThrough(plainServer)
    assert(!plainEngine.tls)

    val appender = new LogAppender("replacing a plaintext engine")
    var tlsEngine: Engine = null
    withLogAppender(appender, Seq(classOf[KyuubiSessionImpl].getName)) {
      tlsEngine = engineThrough(tlsServer)
    }
    assert(tlsEngine.tls)
    assert(tlsEngine.refId !== plainEngine.refId)
    assert(mismatchWarnings(appender, plainEngine).size === 1)
    assertDeregistered(plainEngine)
    assertStops(plainEngine, Some(tlsEngine))
  }

  test("servers with the same engine TLS setting share the engine") {
    val tlsEngine = engineThrough(tlsServer)
    assert(tlsEngine.tls)
    assert(engineThrough(noRetryTlsServer) === tlsEngine)
  }

  test("rolled back, a server without engine TLS replaces the TLS engine") {
    val tlsEngine = engineThrough(tlsServer)
    assert(tlsEngine.tls)

    val appender = new LogAppender("replacing a TLS engine")
    var plainEngine: Engine = null
    withLogAppender(appender, Seq(classOf[KyuubiSessionImpl].getName)) {
      plainEngine = engineThrough(plainServer)
    }
    assert(!plainEngine.tls)
    assert(plainEngine.refId !== tlsEngine.refId)
    assert(mismatchWarnings(appender, tlsEngine).size === 1)
    assertDeregistered(tlsEngine)
    assertStops(tlsEngine, Some(plainEngine))
  }

  test("without retries the client gets the TLS mismatch and the engine is deregistered") {
    val plainEngine = engineThrough(plainServer)
    assert(!plainEngine.tls)

    val e = intercept[SQLException](connect(noRetryTlsServer).close())
    val mismatch = mismatchMessage(plainEngine)
    assert(causes(e).exists(t => Option(t.getMessage).exists(_.contains(mismatch))), e)
    eventually(timeout(30.seconds), interval(500.millis)) {
      assert(sessionsOf(noRetryTlsServer).isEmpty)
    }
    assertDeregistered(plainEngine)
    assertStops(plainEngine, None)
  }

  test("the engine-side spark-defaults.conf cannot turn engine TLS on against the server") {
    val appender = new LogAppender("launching an engine without TLS")
    var plainEngine: Engine = null
    withLogAppender(appender, Seq(classOf[EngineRef].getName, classOf[KyuubiSessionImpl].getName)) {
      plainEngine = engineThrough(plainServer)
    }
    assert(!plainEngine.tls)
    val messages = appender.loggingEvents.map(_.getMessage.getFormattedMessage)
    // a new engine was launched, with the server's decision on its command line
    val launches = messages.filter(_.startsWith("Launching engine"))
    assert(launches.size === 1)
    assert(launches.head.contains(s"spark.${ENGINE_SSL_ENABLED.key}=false"))
    // the engine never started with TLS, so the server had nothing to replace
    assert(!messages.exists(_.contains("was started with TLS on")))
  }
}
