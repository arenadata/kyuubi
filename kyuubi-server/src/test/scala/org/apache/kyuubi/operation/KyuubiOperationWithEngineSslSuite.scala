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

import java.net.{Socket, SocketTimeoutException}
import java.nio.file.{Files, Path}
import java.util.UUID
import javax.net.ssl.SSLSocket

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.logging.log4j.core.LogEvent

import org.apache.kyuubi.{KeyStoreTestUtils, Utils, WithKyuubiServer}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.client.KyuubiSyncThriftClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_TRANSPORT_SSL
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.service.authentication.{InternalSecurityAccessor, PlainSASLHelper}
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TCLIService, TCloseSessionReq, TOpenSessionReq, TStatusCode}
import org.apache.kyuubi.shaded.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.kyuubi.shaded.thrift.transport.{TSaslClientTransport, TSocket, TTransportException}
import org.apache.kyuubi.util.{EngineSslUtils, JavaUtils}

/**
 * A Kyuubi server that connects to its Spark engines over TLS, with engine security on. The
 * engine resolves its key store password through the credential provider of its own
 * core-site.xml, so neither the server nor the engine command line ever carries it.
 */
class KyuubiOperationWithEngineSslSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  import KyuubiOperationWithEngineSslSuite._

  override protected def jdbcUrl: String = getJdbcUrl

  private val secret = s"engine-ssl-secret-${UUID.randomUUID()}"
  private val keyStorePassword = s"engine-key-store-${UUID.randomUUID()}"
  // literals an operator may put in kyuubi-defaults.conf, the server never forwards them
  private val serverLiteralPassword = s"server-literal-${UUID.randomUUID()}"
  private val sparkLiteralPassword = s"spark-literal-${UUID.randomUUID()}"
  // an engine profile that tries to turn engine TLS off and to swap the key store
  private val tlsOffProfile = "engine_ssl_off"
  private val profileKeyStore = s"/tmp/profile-${UUID.randomUUID()}.p12"

  private lazy val keyStore: Path = generateSignedKeyStore(
    materialDir,
    caKeyStore,
    alias = "e2e-engine",
    password = keyStorePassword)

  /**
   * Engines are verified against the host they register: the canonical host name of their
   * address, as in production. The test certificates only cover it when it is a DNS name, see
   * [[KeyStoreTestUtils.defaultSans]]; otherwise engines register their address instead.
   */
  private lazy val engineHostConf: Map[String, String] = {
    val canonicalHost = JavaUtils.findLocalInetAddress.getCanonicalHostName
    if (defaultSans.contains(s"dns:$canonicalHost")) {
      Map.empty
    } else {
      Map(FRONTEND_CONNECTION_URL_USE_HOSTNAME.key -> "false")
    }
  }

  private lazy val engineHadoopConfDir: Path = {
    val dir = Utils.createTempDir("engine-ssl-hadoop-conf")
    val coreSite = new Configuration(false)
    coreSite.set(
      CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
      createCredentialProvider(dir, ENGINE_SSL_KEYSTORE_PASSWORD.key -> keyStorePassword))
    val out = Files.newOutputStream(dir.resolve("core-site.xml"))
    try coreSite.writeXml(out)
    finally out.close()
    dir
  }

  override protected lazy val conf: KyuubiConf = {
    val settings = engineSslServerConf(keyStore) ++ engineHostConf ++ Map(
      ENGINE_SECURITY_ENABLED.key -> "true",
      ENGINE_SECURITY_SECRET_PROVIDER.key -> "simple",
      SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET.key -> secret,
      ENGINE_ALIVE_PROBE_ENABLED.key -> "true",
      s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CONF_DIR" -> engineHadoopConfDir.toString,
      ENGINE_SSL_KEYSTORE_PASSWORD.key -> serverLiteralPassword,
      s"spark.${ENGINE_SSL_KEYSTORE_PASSWORD.key}" -> sparkLiteralPassword,
      s"${ENGINE_PROFILE.key}.$tlsOffProfile.conf.spark.${ENGINE_SSL_ENABLED.key}" -> "false",
      s"${ENGINE_PROFILE.key}.$tlsOffProfile.conf.${ENGINE_SSL_KEYSTORE_PATH.key}" ->
        profileKeyStore)
    val kyuubiConf = KyuubiConf()
    settings.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // KyuubiFunSuite.beforeAll resets it after the server initialized it
    InternalSecurityAccessor.initialize(conf, true)
  }

  override def afterAll(): Unit = {
    try super.afterAll()
    finally InternalSecurityAccessor.reset()
  }

  private def token: String = InternalSecurityAccessor.get().issueToken()

  private def onlySession(): KyuubiSessionImpl = {
    val sessions = sessionsOf(server)
    assert(sessions.size === 1)
    sessions.head
  }

  test("queries and a multi-batch fetch run over TLS between the server and the engine") {
    withJdbcStatement() { statement =>
      val enabled = statement.executeQuery(s"SET spark.${ENGINE_SSL_ENABLED.key}")
      assert(enabled.next())
      assert(enabled.getString(2) === "true")

      // about 20 MB in batches of 1000 rows, each Thrift message spans many TLS records
      val rows = 20000
      val payloadLength = 1024
      statement.setFetchSize(1000)
      val rs = statement.executeQuery(
        s"SELECT id, repeat('x', $payloadLength) AS payload FROM range(0, $rows)")
      var count = 0L
      var idSum = 0L
      var badPayloads = 0
      while (rs.next()) {
        count += 1
        idSum += rs.getLong(1)
        if (rs.getString(2).length != payloadLength) badPayloads += 1
      }
      assert(count === rows)
      assert(idSum === rows.toLong * (rows - 1) / 2)
      assert(badPayloads === 0)

      val session = onlySession()
      assert(session.engineSslEnabled)
      val sockets = engineSockets(session.client)
      assert(sockets.size === 2, "the main and the alive probe connection")
      sockets.foreach { socket =>
        assert(socket.isInstanceOf[SSLSocket])
        val protocol = socket.asInstanceOf[SSLSocket].getSession.getProtocol
        assert(Set("TLSv1.2", "TLSv1.3").contains(protocol))
      }
      val attributes = session.getEngineNode.map(_.attributes)
      assert(attributes.flatMap(_.get(KYUUBI_ENGINE_TRANSPORT_SSL)) === Some("true"))
    }
  }

  test("clients can neither turn off engine TLS nor redirect its verification") {
    Seq(
      Map.empty[String, String] -> Map(ENGINE_SSL_ENABLED.key -> "false"),
      Map(s"spark.${ENGINE_SSL_ENABLED.key}" -> "false") -> Map.empty[String, String],
      Map.empty[String, String] -> Map(ENGINE_SSL_EXPECTED_HOSTNAME.key -> "attacker.invalid"))
      .foreach { case (hiveConfs, hiveVars) =>
        val key = (hiveConfs ++ hiveVars).keys.head
        withSessionConf()(hiveConfs)(hiveVars) {
          val e = assertJDBCConnectionFail()
          assert(
            causes(e).exists(t => Option(t.getMessage).exists(_.contains(s"$key is a restrict"))),
            e)
        }
      }
  }

  test("the engine launch command carries the key store location but no secret") {
    val appender = new LogAppender("launching an engine with TLS")
    withLogAppender(appender, Seq(classOf[EngineRef].getName, classOf[KyuubiSessionImpl].getName)) {
      // a new subdomain makes the server launch an engine while the appender listens
      val subdomain = s"engine-ssl-launch-${UUID.randomUUID().toString.take(8)}"
      withSessionConf()(Map.empty)(Map(ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> subdomain)) {
        withJdbcStatement() { statement =>
          val rs = statement.executeQuery("SELECT 1")
          assert(rs.next())
          assert(rs.getInt(1) === 1)
        }
      }
    }
    val messages = appender.loggingEvents.flatMap(messagesOf)
    val launches = messages.filter(_.startsWith("Launching engine"))
    assert(launches.size === 1)
    val command = launches.head
    assert(command.contains(s"spark.${ENGINE_SSL_ENABLED.key}=true"))
    assert(command.contains(s"spark.${ENGINE_SSL_KEYSTORE_PATH.key}=$keyStore"))
    assert(command.contains(s"spark.${ENGINE_SSL_KEYSTORE_TYPE.key}=${storeTypeOf(keyStore)}"))
    Seq(
      ENGINE_SSL_KEYSTORE_PASSWORD,
      ENGINE_SSL_TRUSTSTORE_PATH,
      ENGINE_SSL_TRUSTSTORE_PASSWORD,
      ENGINE_SSL_EXPECTED_HOSTNAME).map(_.key).foreach { key =>
      assert(!command.contains(key))
    }
    val secrets = Seq(
      keyStorePassword,
      serverLiteralPassword,
      sparkLiteralPassword,
      passwordOf(KeyStoreTestUtils.trustStore))
    secrets.foreach(secret => assert(!messages.exists(_.contains(secret))))
    // the copy from the server defaults is dropped, and only its name is logged
    assert(messages.exists { m =>
      m.contains("Ignoring engine TLS settings") &&
      m.contains(s"spark.${ENGINE_SSL_KEYSTORE_PASSWORD.key}")
    })
  }

  test("engine TLS settings of an engine profile are dropped before the engine sees them") {
    val appender = new LogAppender("an engine profile with engine TLS settings")
    withLogAppender(appender, Seq(classOf[EngineRef].getName, classOf[KyuubiSessionImpl].getName)) {
      val subdomain = s"engine-ssl-profile-${UUID.randomUUID().toString.take(8)}"
      val hiveVars = Map(
        ENGINE_PROFILE.key -> tlsOffProfile,
        ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> subdomain)
      withSessionConf()(Map.empty)(hiveVars) {
        withJdbcStatement() { statement =>
          // the engine would reject the session if the profile keys reached its OpenSession
          val rs = statement.executeQuery("SELECT 1")
          assert(rs.next())
          assert(rs.getInt(1) === 1)
          val session = onlySession()
          assert(session.engineSslEnabled)
          engineSockets(session.client).foreach(socket => assert(socket.isInstanceOf[SSLSocket]))
        }
      }
    }
    val messages = appender.loggingEvents.flatMap(messagesOf)
    val launches = messages.filter(_.startsWith("Launching engine"))
    assert(launches.size === 1)
    assert(launches.head.contains(s"spark.${ENGINE_SSL_ENABLED.key}=true"))
    assert(!launches.head.contains(s"spark.${ENGINE_SSL_ENABLED.key}=false"))
    assert(launches.head.contains(s"spark.${ENGINE_SSL_KEYSTORE_PATH.key}=$keyStore"))
    assert(!launches.head.contains(profileKeyStore))
    val ignored = messages.filter(_.contains("Ignoring engine TLS settings"))
      .map(_.split(": ").last.split(", ").toSet)
    assert(ignored.exists { keys =>
      keys.contains(s"spark.${ENGINE_SSL_ENABLED.key}") &&
      keys.contains(ENGINE_SSL_KEYSTORE_PATH.key)
    })
  }

  test("the engine rejects plaintext clients and serves TLS clients with an engine token") {
    withJdbcStatement() { statement =>
      assert(statement.executeQuery("SELECT 1").next())
      val (host, port) = onlySession().client.hostPort

      val plaintext = intercept[TTransportException] {
        val socket = new TSocket(host, port, TIMEOUT_MS)
        try PlainSASLHelper.getPlainTransport(user, token, socket).open()
        finally socket.close()
      }
      // the engine closes the connection, the client does not wait for its read timeout
      assert(!causes(plaintext).exists(_.isInstanceOf[SocketTimeoutException]), plaintext)

      // verified the way the server verifies it, against the host the engine registered
      val sessionManager = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
      val socket = EngineSslUtils.openClientSocket(
        sessionManager.engineSslClientContext,
        host,
        port,
        TIMEOUT_MS,
        TIMEOUT_MS,
        conf.get(FRONTEND_THRIFT_CLIENT_MAX_MESSAGE_SIZE))
      try {
        val password = token
        val transport = PlainSASLHelper.getPlainTransport(user, password, socket)
        transport.open()
        val client = new TCLIService.Client(new TBinaryProtocol(transport))
        val req = new TOpenSessionReq()
        req.setUsername(user)
        req.setPassword(password)
        val resp = client.OpenSession(req)
        assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS, resp.getStatus)
        client.CloseSession(new TCloseSessionReq(resp.getSessionHandle))
      } finally {
        socket.close()
      }
    }
  }
}

/** Settings and helpers shared by the end-to-end engine TLS suites. */
object KyuubiOperationWithEngineSslSuite {

  val TIMEOUT_MS = 30000

  /**
   * Kyuubi server settings turning engine TLS on: the trust store engine certificates are
   * verified with, and the key store location forwarded to engines. The key store password is
   * not part of them, each suite delivers it to engines its own way.
   */
  def engineSslServerConf(
      keyStore: Path,
      trustStore: Path = KeyStoreTestUtils.trustStore): Map[String, String] =
    serverSslConf(trustStore) ++ Map(
      ENGINE_SSL_KEYSTORE_PATH.key -> keyStore.toString,
      ENGINE_SSL_KEYSTORE_TYPE.key -> storeTypeOf(keyStore))

  def sessionsOf(server: KyuubiServer): Seq[KyuubiSessionImpl] =
    server.backendService.sessionManager.allSessions().collect {
      case session: KyuubiSessionImpl => session
    }.toSeq

  /** The sockets of the main and, if enabled, the alive probe connection to the engine. */
  def engineSockets(client: KyuubiSyncThriftClient): Seq[Socket] =
    (client.getInputProtocol +: client.getEngineAliveProbeProtocol.toSeq).map(socketOf)

  private def socketOf(protocol: TProtocol): Socket =
    protocol.getTransport.asInstanceOf[TSaslClientTransport].getUnderlyingTransport
      .asInstanceOf[TSocket].getSocket

  def causes(e: Throwable): Seq[Throwable] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16).toList

  /** The message of a log event, followed by those of its exception chain. */
  def messagesOf(event: LogEvent): Seq[String] =
    event.getMessage.getFormattedMessage +:
      Option(event.getThrown).toSeq.flatMap(causes).flatMap(t => Option(t.getMessage))

  def hostOf(hostPort: String): String = hostPort.substring(0, hostPort.lastIndexOf(':'))
}
