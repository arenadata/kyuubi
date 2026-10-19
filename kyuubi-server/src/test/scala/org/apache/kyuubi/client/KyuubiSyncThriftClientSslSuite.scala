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

package org.apache.kyuubi.client

import java.net.{ConnectException, InetAddress, ServerSocket, Socket}
import java.util.concurrent.TimeUnit
import javax.net.ssl.{SSLException, SSLSocket}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import org.apache.kyuubi.{KyuubiFunSuite, TcpRecordingRelay, Utils}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.{AbstractNoopServer, NoopSslTBinaryFrontendServer, NoopTBinaryFrontendServer}
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoType, TProtocolVersion}
import org.apache.kyuubi.shaded.thrift.protocol.TProtocol
import org.apache.kyuubi.shaded.thrift.transport.{TSaslClientTransport, TSocket, TTransportException}
import org.apache.kyuubi.util.{EngineSslClientContext, EngineSslUtils, ThreadUtils}

class KyuubiSyncThriftClientSslSuite extends KyuubiFunSuite {

  private val loginTimeoutMs = 10000L
  private val aliveProbeIntervalMs = 5000
  private val user = Utils.currentUser
  private val protocolVersion = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10

  private lazy val clientContext: EngineSslClientContext = {
    val serverConf = KyuubiConf(false)
    serverSslConf().foreach { case (k, v) => serverConf.set(k, v) }
    EngineSslUtils.loadClientContext(serverConf)
  }

  private val engineSecurityConf = Map(
    ENGINE_SECURITY_ENABLED.key -> "true",
    ENGINE_SECURITY_SECRET_PROVIDER.key -> "simple",
    SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET.key -> "_sync_client_ssl_suite_secret_")

  private val clientThreads = ThreadUtils.newDaemonCachedThreadPool("sync-client-ssl-suite")
  private val clientExecution: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(clientThreads)

  override def afterAll(): Unit = {
    try {
      clientThreads.shutdownNow()
      InternalSecurityAccessor.reset()
    } finally {
      super.afterAll()
    }
  }

  private def clientConf(): KyuubiConf =
    KyuubiConf(false).set(ENGINE_LOGIN_TIMEOUT, loginTimeoutMs)

  private def aliveProbeConf(): KyuubiConf =
    clientConf()
      .set(ENGINE_ALIVE_PROBE_ENABLED, true)
      .set(ENGINE_ALIVE_PROBE_INTERVAL, aliveProbeIntervalMs.toLong)

  private def withEngine[T](server: AbstractNoopServer, settings: Map[String, String])(
      f: Int => T): T = {
    val conf = KyuubiConf()
      .set(FRONTEND_THRIFT_BINARY_BIND_HOST.key, "localhost")
      .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    settings.foreach { case (k, v) => conf.set(k, v) }
    server.initialize(conf)
    server.start()
    try {
      val url = server.frontendServices.head.connectionUrl
      f(url.substring(url.lastIndexOf(':') + 1).toInt)
    } finally {
      server.stop()
    }
  }

  private def withEngineSecurity[T](f: => T): T = {
    InternalSecurityAccessor.reset()
    try f
    finally InternalSecurityAccessor.reset()
  }

  private def withClient[T](
      port: Int,
      ssl: Option[EngineSslClientContext],
      password: String = "anonymous",
      conf: KyuubiConf = clientConf())(f: KyuubiSyncThriftClient => T): T = {
    val client = KyuubiSyncThriftClient.createClient(user, password, "localhost", port, conf, ssl)
    try f(client)
    finally client.closeSession()
  }

  /**
   * Open a client that is expected to fail, on another thread and with a deadline: the main
   * engine connection has no read timeout, and a peer that never answers must fail the test
   * instead of blocking the suite.
   *
   * @return the failure and the elapsed time in milliseconds
   */
  private def expectCreateClientFailure(
      port: Int,
      ssl: Option[EngineSslClientContext],
      clientUser: String = user,
      password: String = "anonymous",
      conf: KyuubiConf = clientConf()): (Throwable, Long) = {
    val start = System.nanoTime()
    val attempt = Future(Try(
      KyuubiSyncThriftClient.createClient(clientUser, password, "localhost", port, conf, ssl)))(
      clientExecution)
    val deadline = Duration(2 * conf.get(ENGINE_LOGIN_TIMEOUT), TimeUnit.MILLISECONDS)
    val result = ThreadUtils.awaitResult(attempt, deadline)
    val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
    result match {
      case Success(client) =>
        client.closeSession()
        fail(s"Connected to the engine at port $port")
      case Failure(e) => (e, elapsedMs)
    }
  }

  private def socketOf(protocol: TProtocol): Socket =
    protocol.getTransport.asInstanceOf[TSaslClientTransport].getUnderlyingTransport
      .asInstanceOf[TSocket].getSocket

  private def causes(e: Throwable): Seq[Throwable] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16).toList

  private def checkEngineSession(client: KyuubiSyncThriftClient, password: String): Unit = {
    client.openSession(protocolVersion, user, password, Map.empty)
    val serverName = client.getInfo(TGetInfoType.CLI_SERVER_NAME).getInfoValue.getStringValue
    assert(serverName === "Apache Kyuubi")
  }

  test("the main and the alive probe connections to the engine use TLS") {
    withEngine(new NoopSslTBinaryFrontendServer, engineSslConf()) { port =>
      // neither the Kyuubi nor the Thrift default
      val maxMessageSize = 32 * 1024 * 1024
      val conf = aliveProbeConf().set(FRONTEND_THRIFT_CLIENT_MAX_MESSAGE_SIZE, maxMessageSize)
      val client = KyuubiSyncThriftClient.createClient(
        user,
        "anonymous",
        "localhost",
        port,
        conf,
        Some(clientContext))
      var sockets: Seq[Socket] = Nil
      try {
        val protocols = Seq(client.getInputProtocol) ++ client.getEngineAliveProbeProtocol
        assert(protocols.size === 2)
        sockets = protocols.map(socketOf)
        sockets.foreach { socket =>
          assert(socket.isInstanceOf[SSLSocket])
          val protocol = socket.asInstanceOf[SSLSocket].getSession.getProtocol
          assert(Set("TLSv1.2", "TLSv1.3").contains(protocol))
        }
        // the login timeout only bounds the handshake, then the usual read timeouts apply
        assert(sockets.head.getSoTimeout === 0)
        assert(sockets.last.getSoTimeout === aliveProbeIntervalMs)
        // the SASL transports must see the configured limit, not the Thrift default
        protocols.foreach { protocol =>
          assert(protocol.getTransport.getConfiguration.getMaxMessageSize === maxMessageSize)
        }
        checkEngineSession(client, "anonymous")
      } finally {
        client.closeSession()
      }
      assert(client.engineConnectionClosed)
      assert(sockets.forall(_.isClosed))
    }
  }

  test("without a TLS context the connections to the engine stay plaintext") {
    withEngine(new NoopTBinaryFrontendServer, Map.empty) { port =>
      val client =
        KyuubiSyncThriftClient.createClient(user, "anonymous", "localhost", port, aliveProbeConf())
      try {
        val sockets = (Seq(client.getInputProtocol) ++ client.getEngineAliveProbeProtocol)
          .map(socketOf)
        assert(sockets.size === 2)
        sockets.foreach(socket => assert(!socket.isInstanceOf[SSLSocket]))
        checkEngineSession(client, "anonymous")
      } finally {
        client.closeSession()
      }
    }
  }

  test("a TLS client fails fast against a plaintext engine") {
    withEngine(new NoopTBinaryFrontendServer, Map.empty) { port =>
      val (e, elapsedMs) = expectCreateClientFailure(port, Some(clientContext))
      assert(e.isInstanceOf[TTransportException])
      assert(e.getMessage.contains(s"TLS handshake with engine localhost:$port failed"))
      // the engine answers the ClientHello with a SASL error, and may reset the connection
      // because it closes it without reading the rest of the ClientHello
      assert(Seq("not speaking TLS", "Connection reset").exists(e.getMessage.contains))
      assert(elapsedMs < loginTimeoutMs)
    }
  }

  test("a plaintext client fails fast against a TLS engine") {
    withEngine(new NoopSslTBinaryFrontendServer, engineSslConf()) { port =>
      val (e, elapsedMs) = expectCreateClientFailure(port, None)
      assert(e.isInstanceOf[TTransportException])
      assert(elapsedMs < loginTimeoutMs)
    }
  }

  test("the TLS handshake is bounded by the engine login timeout") {
    // accepts connections in its backlog but never answers
    val silentPeer = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    try {
      val loginTimeout = 2000L
      val conf = clientConf().set(ENGINE_LOGIN_TIMEOUT, loginTimeout)
      val (e, elapsedMs) =
        expectCreateClientFailure(silentPeer.getLocalPort, Some(clientContext), conf = conf)
      assert(e.isInstanceOf[TTransportException])
      assert(e.getMessage.contains(s"timed out after $loginTimeout ms"))
      assert(elapsedMs < 2 * loginTimeout)
    } finally {
      silentPeer.close()
    }
  }

  test("a closed engine port keeps the 'Connection refused' failure that is retried") {
    val closedPort = {
      val socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress)
      try socket.getLocalPort
      finally socket.close()
    }
    Seq(None, Some(clientContext)).foreach { ssl =>
      val (e, _) = expectCreateClientFailure(closedPort, ssl)
      // the exact shape KyuubiSessionImpl.openEngineSession retries on
      assert(e.isInstanceOf[TTransportException])
      assert(e.getCause.isInstanceOf[ConnectException])
      assert(e.getCause.getMessage.contains("Connection refused"))
    }
  }

  test("an untrusted engine certificate fails before any credentials are sent") {
    val clientUser = "untrusted-engine-user"
    val password = "untrusted-engine-secret"
    withEngine(new NoopSslTBinaryFrontendServer, engineSslConf(foreignKeyStore)) { port =>
      val relay = new TcpRecordingRelay("localhost", port)
      try {
        val (e, _) =
          expectCreateClientFailure(relay.port, Some(clientContext), clientUser, password)
        assert(e.isInstanceOf[TTransportException])
        assert(e.getMessage.contains(s"not trusted by $trustStore"))
        assert(causes(e).exists(_.isInstanceOf[SSLException]))
        // only the TLS handshake reached the engine, no SASL message
        assert(relay.clientBytes.headOption.contains(0x16.toByte))
        Seq("PLAIN", clientUser, password).foreach { text =>
          assert(!relay.contains(text), s"$text went over the wire")
        }
      } finally {
        relay.close()
      }
    }
  }

  test("the max message size applies to TLS connections to the engine") {
    withEngine(new NoopSslTBinaryFrontendServer, engineSslConf()) { port =>
      // smaller than the name of the OpenSession reply
      val tiny = clientConf().set(FRONTEND_THRIFT_CLIENT_MAX_MESSAGE_SIZE, 8)
      withClient(port, Some(clientContext), conf = tiny) { client =>
        val e = intercept[TTransportException] {
          client.openSession(protocolVersion, user, "anonymous", Map.empty)
        }
        assert(e.getMessage.contains("MaxMessageSize reached"))
      }
      withClient(port, Some(clientContext))(checkEngineSession(_, "anonymous"))
    }
  }

  test("the engine access token is checked over TLS") {
    withEngineSecurity {
      withEngine(new NoopSslTBinaryFrontendServer, engineSslConf() ++ engineSecurityConf) {
        port =>
          val token = InternalSecurityAccessor.get().issueToken()
          withClient(port, Some(clientContext), token)(checkEngineSession(_, token))
          val forged = "0123456789abcdef" * 4
          val (e, _) = expectCreateClientFailure(port, Some(clientContext), password = forged)
          assert(e.isInstanceOf[TTransportException])
      }
    }
  }

  test("an invalid engine trust store fails the session manager at startup") {
    val conf = KyuubiConf(false)
      .set(ENGINE_SSL_ENABLED, true)
      .set(ENGINE_SSL_TRUSTSTORE_PATH.key, "/nonexistent/engine-truststore.p12")
    val e = intercept[IllegalArgumentException](new KyuubiSessionManager().initialize(conf))
    assert(e.getMessage.contains(ENGINE_SSL_TRUSTSTORE_PATH.key))
    assert(e.getMessage.contains("/nonexistent/engine-truststore.p12"))
  }
}
