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

package org.apache.kyuubi.server.grpc

import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import javax.net.ssl.{SSLHandshakeException, SSLSession}

import scala.collection.JavaConverters._

import io.grpc.{Grpc, ManagedChannel, Metadata, Server, ServerCall, ServerCallHandler, ServerInterceptor, ServerInterceptors, Status, StatusRuntimeException, TlsServerCredentials}
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, never, times, verify, when}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.spark.connect.proto.{ConfigRequest, ConfigResponse, SparkConnectServiceGrpc}
import org.apache.kyuubi.util.EngineSslUtils

/**
 * The Spark Connect channel from the Kyuubi server to an engine, built by the real
 * [[SparkConnectSessionManager.buildChannel]] against an in-JVM gRPC engine stand-in.
 */
class SparkConnectSessionManagerTlsSuite extends KyuubiFunSuite with MockitoSugar {

  private val callDeadlineSeconds = 10L
  // An IP literal resolves to a single address, so a failed call reports the TLS error of the
  // engine, not a refused connection to another address that localhost resolves to.
  private val loopback = "127.0.0.1"

  private val engineSecurityConf = Seq(
    ENGINE_SECURITY_ENABLED.key -> "true",
    ENGINE_SECURITY_SECRET_PROVIDER.key -> "simple",
    SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET.key -> "_connect_session_manager_tls_suite_")

  override def beforeAll(): Unit = {
    super.beforeAll()
    InternalSecurityAccessor.reset()
  }

  override def afterAll(): Unit = {
    try InternalSecurityAccessor.reset()
    finally super.afterAll()
  }

  /** What the engine stand-in observed of one call that reached it. */
  private case class EngineCall(sslSession: Option[SSLSession], token: Option[String])

  /**
   * A Spark Connect engine stand-in listening on the loopback address. It serves TLS with the
   * given key store, or plaintext without one, and records the TLS session and the engine
   * token of every call that reaches it.
   */
  private class FakeEngine(keyStore: Option[Path]) extends AutoCloseable {

    val calls = new ConcurrentLinkedQueue[EngineCall]()

    private val recorder = new ServerInterceptor {
      override def interceptCall[ReqT, RespT](
          call: ServerCall[ReqT, RespT],
          headers: Metadata,
          next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
        calls.add(EngineCall(
          Option(call.getAttributes.get(Grpc.TRANSPORT_ATTR_SSL_SESSION)),
          Option(headers.get(KyuubiEngineTokenClientInterceptor.TOKEN_HEADER))))
        next.startCall(call, headers)
      }
    }

    private val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase {
      override def config(
          request: ConfigRequest,
          responseObserver: StreamObserver[ConfigResponse]): Unit = {
        responseObserver.onNext(
          ConfigResponse.newBuilder().setSessionId(request.getSessionId).build())
        responseObserver.onCompleted()
      }
    }

    private val server: Server = {
      val address = new InetSocketAddress(InetAddress.getByName(loopback), 0)
      val builder = keyStore match {
        case Some(store) =>
          val credentials =
            TlsServerCredentials.newBuilder().keyManager(keyManagers(store): _*).build()
          NettyServerBuilder.forAddress(address, credentials)
        case None => NettyServerBuilder.forAddress(address)
      }
      builder.addService(ServerInterceptors.intercept(service, recorder)).build().start()
    }

    def port: Int = server.getPort

    override def close(): Unit = {
      server.shutdownNow()
      server.awaitTermination(callDeadlineSeconds, TimeUnit.SECONDS)
    }
  }

  private def withEngine[T](keyStore: Option[Path])(f: FakeEngine => T): T = {
    val engine = new FakeEngine(keyStore)
    try f(engine)
    finally engine.close()
  }

  private def serverConf(settings: Seq[(String, String)]): KyuubiConf = {
    val conf = KyuubiConf(false)
    settings.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  /**
   * A mocked KyuubiSessionManager that hands out the trust material loaded from the server
   * conf, as the real one does at startup.
   */
  private def kyuubiSessionManager(conf: KyuubiConf): KyuubiSessionManager = {
    val sessionManager = mock[KyuubiSessionManager]
    when(sessionManager.getConf).thenReturn(conf)
    if (conf.get(ENGINE_SSL_ENABLED)) {
      when(sessionManager.engineSslClientContext)
        .thenReturn(EngineSslUtils.loadClientContext(conf))
    }
    sessionManager
  }

  /**
   * A SparkConnectSessionManager with the real buildChannel. Every session it opens is backed
   * by a Kyuubi session whose engine registered the given Spark Connect URL.
   */
  private def withManager[T](sessionManager: SessionManager, connectUrl: String)(
      f: (SparkConnectSessionManager, BackendService) => T): T = {
    val backendService = mock[BackendService]
    val kyuubiSession = mock[KyuubiSessionImpl]
    when(backendService.openSession(any(), any(), any(), any(), any()))
      .thenReturn(SessionHandle(UUID.randomUUID()))
    when(backendService.sessionManager).thenReturn(sessionManager)
    when(sessionManager.getSession(any[SessionHandle]())).thenReturn(kyuubiSession)
    doNothing().when(kyuubiSession).waitForEngineLaunched()
    when(kyuubiSession.engineConnectUrl).thenReturn(Some(connectUrl))
    val manager = new SparkConnectSessionManager(backendService)
    try f(manager, backendService)
    finally manager.closeAll()
  }

  private def callConfig(channel: ManagedChannel, sessionId: String): ConfigResponse =
    SparkConnectServiceGrpc.newBlockingStub(channel)
      .withDeadlineAfter(callDeadlineSeconds, TimeUnit.SECONDS)
      .config(ConfigRequest.newBuilder().setSessionId(sessionId).build())

  /** Calls the engine through a new session, expecting the call to fail before the deadline. */
  private def expectCallFailure(
      manager: SparkConnectSessionManager,
      engine: FakeEngine): StatusRuntimeException = {
    val session = manager.getOrOpen("failing", "john")
    val e = intercept[StatusRuntimeException](callConfig(session.channel, "failing"))
    assert(e.getStatus.getCode !== Status.Code.DEADLINE_EXCEEDED, e)
    assert(engine.calls.isEmpty, "no call may reach the engine")
    e
  }

  private def causes(e: Throwable): Seq[Throwable] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16).toList

  private def assertTls(call: EngineCall): Unit = {
    assert(call.sslSession.isDefined, "the call must arrive over TLS")
    assert(Set("TLSv1.2", "TLSv1.3").contains(call.sslSession.get.getProtocol))
  }

  test("TLS channel to a trusted engine, trust material shared by all sessions") {
    withEngine(Some(goodKeyStore)) { engine =>
      val sessionManager = kyuubiSessionManager(serverConf(serverSslConf().toSeq))
      // reached by its DNS name, which the engine certificate holds as a SAN
      withManager(sessionManager, s"localhost:${engine.port}") { (manager, _) =>
        Seq("s1", "s2").foreach { sessionId =>
          val session = manager.getOrOpen(sessionId, "john")
          assert(callConfig(session.channel, sessionId).getSessionId === sessionId)
        }
        assert(engine.calls.size === 2)
        engine.calls.asScala.foreach { call =>
          assertTls(call)
          assert(call.token.isEmpty, "no engine token without engine security")
        }
        verify(sessionManager, times(1)).engineSslClientContext
      }
    }
  }

  test("engine token travels inside the TLS channel and validates") {
    val conf = serverConf(serverSslConf().toSeq ++ engineSecurityConf)
    InternalSecurityAccessor.reset()
    InternalSecurityAccessor.initialize(conf, isServer = true)
    try {
      withEngine(Some(goodKeyStore)) { engine =>
        val target = s"$loopback:${engine.port}"
        withManager(kyuubiSessionManager(conf), target) { (manager, _) =>
          val session = manager.getOrOpen("token", "john")
          assert(callConfig(session.channel, "token").getSessionId === "token")
          val call = engine.calls.asScala.toList match {
            case single :: Nil => single
            case other => fail(s"expected one call, got ${other.size}")
          }
          assertTls(call)
          assert(call.token.isDefined, "the engine token header must be sent")
          InternalSecurityAccessor.get().authToken(call.token.get)
        }
      }
    } finally {
      InternalSecurityAccessor.reset()
    }
  }

  test("untrusted engine certificate fails with an SSL handshake error") {
    withEngine(Some(foreignKeyStore)) { engine =>
      val sessionManager = kyuubiSessionManager(serverConf(serverSslConf().toSeq))
      withManager(sessionManager, s"$loopback:${engine.port}") { (manager, _) =>
        val e = expectCallFailure(manager, engine)
        assert(e.getStatus.getCode === Status.Code.UNAVAILABLE, e)
        assert(causes(e).exists(_.isInstanceOf[SSLHandshakeException]), e)
      }
    }
  }

  test("engine host name is verified, kyuubi.engine.ssl.expected.hostname replaces it") {
    withEngine(Some(localhostOnlyKeyStore)) { engine =>
      // the certificate is only valid for the DNS name localhost, not for the loopback address
      val target = s"$loopback:${engine.port}"
      withManager(kyuubiSessionManager(serverConf(serverSslConf().toSeq)), target) {
        (manager, _) =>
          val e = expectCallFailure(manager, engine)
          assert(e.getStatus.getCode === Status.Code.UNAVAILABLE, e)
          assert(
            causes(e).exists(c =>
              Option(c.getMessage).exists(m =>
                m.contains("No subject alternative") || m.contains("No name matching"))),
            e)
      }

      val expectedHostname = serverConf(
        serverSslConf().toSeq :+ (ENGINE_SSL_EXPECTED_HOSTNAME.key -> "localhost"))
      withManager(kyuubiSessionManager(expectedHostname), target) { (manager, _) =>
        val session = manager.getOrOpen("expected-hostname", "john")
        assert(callConfig(session.channel, "expected-hostname").getSessionId ===
          "expected-hostname")
        assert(engine.calls.size === 1)
        assertTls(engine.calls.peek())
      }
    }
  }

  test("TLS channel never falls back to plaintext against a plaintext engine") {
    withEngine(None) { engine =>
      val sessionManager = kyuubiSessionManager(serverConf(serverSslConf().toSeq))
      withManager(sessionManager, s"$loopback:${engine.port}") { (manager, _) =>
        expectCallFailure(manager, engine)
      }
    }
  }

  test("plaintext channel cannot reach a TLS engine") {
    withEngine(Some(goodKeyStore)) { engine =>
      val sessionManager = kyuubiSessionManager(serverConf(Nil))
      withManager(sessionManager, s"$loopback:${engine.port}") { (manager, _) =>
        expectCallFailure(manager, engine)
      }
    }
  }

  test("engine TLS off keeps the plaintext channel and never loads trust material") {
    withEngine(None) { engine =>
      val conf = serverConf(Seq(
        ENGINE_SSL_ENABLED.key -> "false",
        ENGINE_SSL_TRUSTSTORE_PATH.key -> "/nonexistent/truststore.p12"))
      val sessionManager = kyuubiSessionManager(conf)
      withManager(sessionManager, s"$loopback:${engine.port}") { (manager, _) =>
        val session = manager.getOrOpen("plain", "john")
        assert(callConfig(session.channel, "plain").getSessionId === "plain")
        assert(engine.calls.size === 1)
        assert(engine.calls.peek().sslSession.isEmpty)
        verify(sessionManager, never()).engineSslClientContext
      }

      // with TLS off any session manager works, as before engine TLS existed
      val plainSessionManager = mock[SessionManager]
      when(plainSessionManager.getConf).thenReturn(conf)
      withManager(plainSessionManager, s"$loopback:${engine.port}") { (manager, _) =>
        val session = manager.getOrOpen("plain-session-manager", "john")
        assert(callConfig(session.channel, "plain-session-manager").getSessionId ===
          "plain-session-manager")
      }
      assert(engine.calls.size === 2)
      assert(engine.calls.asScala.forall(_.sslSession.isEmpty))
    }
  }

  test("engine TLS on without a KyuubiSessionManager fails and closes the session") {
    val sessionManager = mock[SessionManager]
    when(sessionManager.getConf).thenReturn(serverConf(serverSslConf().toSeq))
    withManager(sessionManager, s"$loopback:1") { (manager, backendService) =>
      val e = intercept[KyuubiException](manager.getOrOpen("no-kyuubi", "john"))
      assert(e.getMessage.contains(ENGINE_SSL_ENABLED.key))
      verify(backendService).closeSession(any())
    }
  }
}
