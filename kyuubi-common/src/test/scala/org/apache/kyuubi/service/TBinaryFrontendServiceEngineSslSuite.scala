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

package org.apache.kyuubi.service

import java.io.IOException
import javax.net.ssl.{SSLContext, SSLSocket}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.kyuubi.{BoundedCall, KyuubiException, KyuubiFunSuite, TcpRecordingRelay, Utils}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_CREDENTIALS_KEY
import org.apache.kyuubi.service.authentication.{InternalSecurityAccessor, PlainSASLHelper}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.thrift.protocol.TBinaryProtocol
import org.apache.kyuubi.shaded.thrift.transport.{TSocket, TTransportException}
import org.apache.kyuubi.util.{EngineSslClientContext, EngineSslUtils}

class TBinaryFrontendServiceEngineSslSuite extends KyuubiFunSuite {

  private val timeoutMs = 10000
  private val maxMessageSize = 64 * 1024 * 1024

  private lazy val clientContext: EngineSslClientContext =
    EngineSslUtils.loadClientContext(conf(serverSslConf()))

  private val engineSecurityConf = Map(
    ENGINE_SECURITY_ENABLED.key -> "true",
    ENGINE_SECURITY_SECRET_PROVIDER.key -> "simple",
    SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET.key -> "_engine_ssl_suite_secret_")

  private lazy val frontendSslConf = Map(
    FRONTEND_THRIFT_BINARY_SSL_ENABLED.key -> "true",
    FRONTEND_SSL_KEYSTORE_PATH.key -> goodKeyStore.toString,
    FRONTEND_SSL_KEYSTORE_PASSWORD.key -> passwordOf(goodKeyStore),
    FRONTEND_SSL_KEYSTORE_TYPE.key -> storeTypeOf(goodKeyStore))

  override def afterAll(): Unit = {
    InternalSecurityAccessor.reset()
    super.afterAll()
  }

  private def conf(settings: Map[String, String]): KyuubiConf = {
    val kyuubiConf = KyuubiConf()
      .set(FRONTEND_THRIFT_BINARY_BIND_HOST.key, "localhost")
      .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    settings.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  private def withServer[T](
      server: AbstractNoopServer,
      settings: Map[String, String])(f: TBinaryFrontendService => T): T = {
    server.initialize(conf(settings))
    server.start()
    try f(server.frontendServices.head.asInstanceOf[TBinaryFrontendService])
    finally server.stop()
  }

  private def withEngineSecurity[T](f: => T): T = {
    InternalSecurityAccessor.reset()
    try f
    finally InternalSecurityAccessor.reset()
  }

  private def portOf(fe: TBinaryFrontendService): Int = {
    val url = fe.connectionUrl
    url.substring(url.lastIndexOf(':') + 1).toInt
  }

  private def withTlsClient[T](
      port: Int,
      user: String = Utils.currentUser,
      password: String = "anonymous",
      ctx: EngineSslClientContext = clientContext)(f: (TCLIService.Iface, SSLSocket) => T): T = {
    val socket = EngineSslUtils.openClientSocket(
      ctx,
      "localhost",
      port,
      timeoutMs,
      timeoutMs,
      maxMessageSize)
    try {
      val transport = PlainSASLHelper.getPlainTransport(user, password, socket)
      transport.open()
      val client = new TCLIService.Client(new TBinaryProtocol(transport))
      f(client, socket.getSocket.asInstanceOf[SSLSocket])
    } finally {
      socket.close()
    }
  }

  private def withPlainClient[T](
      port: Int,
      user: String = Utils.currentUser,
      password: String = "anonymous")(f: TCLIService.Iface => T): T = {
    val socket = new TSocket("localhost", port, timeoutMs)
    try {
      val transport = PlainSASLHelper.getPlainTransport(user, password, socket)
      transport.open()
      f(new TCLIService.Client(new TBinaryProtocol(transport)))
    } finally {
      socket.close()
    }
  }

  private def checkSession(
      client: TCLIService.Iface,
      sessionConf: Map[String, String] = Map.empty,
      user: String = Utils.currentUser,
      password: String = "anonymous"): Unit = {
    val req = new TOpenSessionReq()
    req.setUsername(user)
    req.setPassword(password)
    req.setConfiguration(sessionConf.asJava)
    val resp = client.OpenSession(req)
    assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    val handle = resp.getSessionHandle
    val info = client.GetInfo(new TGetInfoReq(handle, TGetInfoType.CLI_SERVER_NAME))
    assert(info.getInfoValue.getStringValue === "Apache Kyuubi")
    val closed = client.CloseSession(new TCloseSessionReq(handle))
    assert(closed.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
  }

  test("engine TLS listener serves PLAIN SASL sessions") {
    withServer(new NoopSslTBinaryFrontendServer, engineSslConf()) { fe =>
      assert(fe.engineSslEnabled)
      withTlsClient(portOf(fe)) { (client, ssl) =>
        assert(Set("TLSv1.2", "TLSv1.3").contains(ssl.getSession.getProtocol))
        checkSession(client)
      }
    }
  }

  test("a plaintext client is rejected and the TLS listener keeps serving") {
    withServer(new NoopSslTBinaryFrontendServer, engineSslConf()) { fe =>
      val start = System.nanoTime()
      BoundedCall.within(60.seconds) {
        intercept[TTransportException](withPlainClient(portOf(fe))(checkSession(_)))
      }
      assert((System.nanoTime() - start) / 1000000 < 2 * timeoutMs)
      withTlsClient(portOf(fe))((client, _) => checkSession(client))
    }
  }

  test("untrusted, wrong host and expired engine certificates fail at the client") {
    Seq(
      foreignKeyStore -> s"not trusted by $trustStore",
      otherHostKeyStore -> "does not match localhost",
      expiredKeyStore -> "has expired").foreach { case (keyStore, reason) =>
      withServer(new NoopSslTBinaryFrontendServer, engineSslConf(keyStore)) { fe =>
        val e = intercept[TTransportException] {
          BoundedCall.within(60.seconds)(withTlsClient(portOf(fe))((client, _) =>
            checkSession(client)))
        }
        assert(e.getMessage.contains(reason))
      }
    }
  }

  test("disallowed protocols are not negotiated") {
    assume(SSLContext.getDefault.getSupportedSSLParameters.getProtocols.contains("TLSv1.3"))
    val settings =
      engineSslConf() + (ENGINE_SSL_DISALLOWED_PROTOCOLS.key -> "SSLv3,TLSv1,TLSv1.1,TLSv1.2")
    withServer(new NoopSslTBinaryFrontendServer, settings) { fe =>
      val pinned = clientContext.socketFactory.createSocket("localhost", portOf(fe))
        .asInstanceOf[SSLSocket]
      try {
        pinned.setSoTimeout(timeoutMs)
        pinned.setEnabledProtocols(Array("TLSv1.2"))
        intercept[IOException](pinned.startHandshake())
      } finally {
        pinned.close()
      }
      withTlsClient(portOf(fe)) { (client, ssl) =>
        assert(ssl.getSession.getProtocol === "TLSv1.3")
        checkSession(client)
      }
    }
  }

  test("only the included cipher suites are negotiated") {
    val suite = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
    assume(SSLContext.getDefault.getSupportedSSLParameters.getCipherSuites.contains(suite))
    val settings = engineSslConf() +
      (ENGINE_SSL_INCLUDE_CIPHER_SUITES.key -> s"$suite,TLS_NOT_A_CIPHER_SUITE")
    withServer(new NoopSslTBinaryFrontendServer, settings) { fe =>
      withTlsClient(portOf(fe)) { (client, ssl) =>
        assert(ssl.getSession.getCipherSuite === suite)
        checkSession(client)
      }
    }
  }

  test("frontends without engine TLS support stay plaintext") {
    Seq(engineSslConf(), frontendSslConf).foreach { settings =>
      withServer(new NoopTBinaryFrontendServer, settings) { fe =>
        assert(!fe.engineSslEnabled)
        withPlainClient(portOf(fe))(checkSession(_))
      }
    }
  }

  test("engine TLS is off by default and ignores the frontend SSL settings") {
    Seq(Map.empty[String, String], frontendSslConf).foreach { settings =>
      withServer(new NoopSslTBinaryFrontendServer, settings) { fe =>
        assert(!fe.engineSslEnabled)
        withPlainClient(portOf(fe))(checkSession(_))
      }
    }
  }

  test("the server side ignores engine TLS settings and keeps its frontend SSL") {
    val bogusEngineSsl = engineSslConf() + (ENGINE_SSL_KEYSTORE_PATH.key -> "/nonexistent.p12")
    withServer(new ServerSideNoopServer, bogusEngineSsl) { fe =>
      assert(!fe.engineSslEnabled)
      withPlainClient(portOf(fe))(checkSession(_))
    }

    // with an untrusted engine key store, a handshake that used it would fail
    withServer(new ServerSideNoopServer, frontendSslConf ++ engineSslConf(foreignKeyStore)) { fe =>
      assert(!fe.engineSslEnabled)
      assert(fe.asInstanceOf[ServerSideFrontendService].frontendKeyStore === ((
        Some(goodKeyStore.toString),
        Some(passwordOf(goodKeyStore)),
        Some(storeTypeOf(goodKeyStore)))))
      withTlsClient(portOf(fe))((client, _) => checkSession(client))
    }
  }

  test("a missing engine key store fails the frontend initialization") {
    val fe = new NoopSslTBinaryFrontendService(new NoopTBinaryFrontendServer)
    val e = intercept[KyuubiException] {
      fe.initialize(conf(engineSslConf() - ENGINE_SSL_KEYSTORE_PATH.key))
    }
    assert(e.getCause.isInstanceOf[IllegalArgumentException])
    assert(e.getCause.getMessage.contains(ENGINE_SSL_KEYSTORE_PATH.key))
    assert(!fe.engineSslEnabled)
  }

  test("the engine access token is checked over TLS") {
    withEngineSecurity {
      withServer(new NoopSslTBinaryFrontendServer, engineSslConf() ++ engineSecurityConf) { fe =>
        val token = InternalSecurityAccessor.get().issueToken()
        withTlsClient(portOf(fe), password = token) { (client, _) =>
          checkSession(client, password = token)
        }
        val forged = "0123456789abcdef" * 4
        intercept[TTransportException] {
          withTlsClient(portOf(fe), password = forged)((client, _) => checkSession(client))
        }
      }
    }
  }

  test("the user, the engine token and the session conf are not readable on the wire") {
    val user = "wire-proof-user"
    val marker = "wire-proof-credentials-marker"
    val sessionConf = Map(KYUUBI_ENGINE_CREDENTIALS_KEY -> marker)

    withEngineSecurity {
      withServer(new NoopSslTBinaryFrontendServer, engineSslConf() ++ engineSecurityConf) { fe =>
        val token = InternalSecurityAccessor.get().issueToken()
        val relay = new TcpRecordingRelay("localhost", portOf(fe))
        try {
          withTlsClient(relay.port, user, token) { (client, _) =>
            checkSession(client, sessionConf, user, token)
          }
          // a TLS handshake record opens the connection
          assert(relay.clientBytes.headOption.contains(0x16.toByte))
          Seq(user, token, marker, KYUUBI_ENGINE_CREDENTIALS_KEY).foreach { text =>
            assert(!relay.contains(text), s"$text is readable on the wire")
          }
        } finally {
          relay.close()
        }
      }

      // positive control: the same exchange in plaintext shows all of them
      withServer(new NoopTBinaryFrontendServer, engineSecurityConf) { fe =>
        val token = InternalSecurityAccessor.get().issueToken()
        val relay = new TcpRecordingRelay("localhost", portOf(fe))
        try {
          withPlainClient(relay.port, user, token)(checkSession(_, sessionConf, user, token))
          Seq(user, token, marker, KYUUBI_ENGINE_CREDENTIALS_KEY).foreach { text =>
            assert(relay.contains(text), s"$text is not readable on the wire")
          }
        } finally {
          relay.close()
        }
      }
    }
  }
}

/** A Kyuubi server side Thrift binary frontend, even though it claims engine TLS support. */
private[service] class ServerSideFrontendService(serverable: Serverable)
  extends NoopSslTBinaryFrontendService(serverable) {

  override protected def isServer(): Boolean = true

  def frontendKeyStore: (Option[String], Option[String], Option[String]) =
    (keyStorePath, keyStorePassword, keyStoreType)
}

private[service] class ServerSideNoopServer extends AbstractNoopServer("ServerSideNoopServer") {

  override val frontendServices = Seq(new ServerSideFrontendService(this))
}
