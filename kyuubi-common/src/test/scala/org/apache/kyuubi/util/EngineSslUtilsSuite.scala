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

package org.apache.kyuubi.util

import java.io.{EOFException, IOException}
import java.net.{ConnectException, InetAddress, ServerSocket, SocketTimeoutException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermission
import java.security.cert.{CertificateException, CertificateExpiredException}
import java.util.Collections
import javax.net.ssl.{SSLServerSocket, SSLSocket}

import scala.concurrent.duration._
import scala.util.Try

import org.apache.hadoop.security.alias.CredentialProviderFactory

import org.apache.kyuubi.{BoundedCall, KyuubiFunSuite, Utils}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.shaded.thrift.transport.{TSocket, TTransportException}

class EngineSslUtilsSuite extends KyuubiFunSuite {

  private val timeoutMs = 10000
  private val maxMessageSize = 64 * 1024 * 1024
  private lazy val workDir: Path = Utils.createTempDir("engine-ssl-utils")
  private lazy val clientContext: EngineSslClientContext =
    EngineSslUtils.loadClientContext(conf(serverSslConf()))

  private def conf(settings: Map[String, String]): KyuubiConf = {
    val kyuubiConf = KyuubiConf(loadSysDefault = false)
    settings.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  /**
   * Run a TLS listener with the given key store that completes the handshake of each client
   * and then reads until the client goes away.
   */
  private def withTlsServer[T](
      keyStore: Path,
      extraConf: Map[String, String] = Map.empty)(f: Int => T): T = {
    val material = EngineSslUtils.loadServerMaterial(conf(engineSslConf(keyStore) ++ extraConf))
    val serverSocket = EngineSslUtils.newServerSocket(material, 0, InetAddress.getLoopbackAddress)
      .getServerSocket
    val acceptor = new Thread(
      new Runnable {
        override def run(): Unit = {
          while (!serverSocket.isClosed) {
            Try(serverSocket.accept()).foreach { socket =>
              try {
                socket.setSoTimeout(timeoutMs)
                socket.asInstanceOf[SSLSocket].startHandshake()
                val in = socket.getInputStream
                while (in.read() >= 0) {}
              } catch {
                case _: IOException =>
              } finally {
                socket.close()
              }
            }
          }
        }
      },
      "engine-ssl-utils-suite-tls-server")
    acceptor.setDaemon(true)
    acceptor.start()
    try f(serverSocket.getLocalPort)
    finally {
      serverSocket.close()
      acceptor.join(timeoutMs)
    }
  }

  private def connect(
      port: Int,
      host: String = "localhost",
      ctx: EngineSslClientContext = clientContext,
      connectTimeoutMs: Int = timeoutMs): TSocket = BoundedCall.within(60.seconds) {
    EngineSslUtils.openClientSocket(ctx, host, port, timeoutMs, connectTimeoutMs, maxMessageSize)
  }

  private def connectFailure(
      port: Int,
      host: String = "localhost",
      ctx: EngineSslClientContext = clientContext): TTransportException = {
    val e = intercept[TTransportException](connect(port, host, ctx))
    assert(e.getType === TTransportException.NOT_OPEN)
    e
  }

  private def messages(e: Throwable): Seq[String] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).map(t => String.valueOf(t.getMessage))
      .toList

  test("load PKCS12 and JKS key stores") {
    Seq(goodKeyStore, goodJksKeyStore).foreach { keyStore =>
      val material = EngineSslUtils.loadServerMaterial(conf(engineSslConf(keyStore)))
      assert(material.keyManagerFactory.getKeyManagers.nonEmpty)
      assert(material.protocols.nonEmpty)
      assert(material.protocols.intersect(Seq("SSLv2", "SSLv3", "TLSv1", "TLSv1.1")).isEmpty)
      assert(material.cipherSuites.isEmpty)
      withTlsServer(keyStore) { port =>
        val socket = connect(port)
        try assert(socket.isOpen)
        finally socket.close()
      }
    }
  }

  test("a missing key store path or password names the key") {
    val noPath = intercept[IllegalArgumentException] {
      EngineSslUtils.loadServerMaterial(conf(engineSslConf() - ENGINE_SSL_KEYSTORE_PATH.key))
    }
    assert(noPath.getMessage.contains(ENGINE_SSL_KEYSTORE_PATH.key))

    val noPassword = intercept[IllegalArgumentException] {
      EngineSslUtils.loadServerMaterial(conf(engineSslConf() - ENGINE_SSL_KEYSTORE_PASSWORD.key))
    }
    assert(noPassword.getMessage.contains(ENGINE_SSL_KEYSTORE_PASSWORD.key))
    assert(noPassword.getMessage.contains("credential provider alias"))
    assert(noPassword.getMessage.contains(s"spark.${ENGINE_SSL_KEYSTORE_PASSWORD.key}"))
  }

  test("a wrong key store password is reported without its value") {
    val wrongPassword = "Wr0ng-Engine-Password"
    Seq(goodKeyStore, goodJksKeyStore).foreach { keyStore =>
      val e = intercept[IllegalArgumentException] {
        EngineSslUtils.loadServerMaterial(
          conf(engineSslConf(keyStore) + (ENGINE_SSL_KEYSTORE_PASSWORD.key -> wrongPassword)))
      }
      assert(e.getMessage.contains(ENGINE_SSL_KEYSTORE_PASSWORD.key))
      assert(e.getMessage.contains(keyStore.toString))
      assert(!messages(e).exists(_.contains(wrongPassword)))
    }
  }

  test("a missing or unreadable key store names the path, the user and the host") {
    val host = Try(InetAddress.getLocalHost.getHostName).getOrElse("<unknown>")
    val user = System.getProperty("user.name")

    val missing = workDir.resolve("missing.p12")
    val e1 = intercept[IllegalArgumentException] {
      EngineSslUtils.loadServerMaterial(
        conf(engineSslConf() + (ENGINE_SSL_KEYSTORE_PATH.key -> missing.toString)))
    }
    Seq(missing.toString, "does not exist", user, host, "NodeManager").foreach { part =>
      assert(e1.getMessage.contains(part))
    }

    val unreadable = workDir.resolve("unreadable.p12")
    Files.copy(goodKeyStore, unreadable)
    Try(Files.setPosixFilePermissions(unreadable, Collections.emptySet[PosixFilePermission]))
    // root, or a file system without POSIX permissions, can still read the file
    assume(!Files.isReadable(unreadable))
    val e2 = intercept[IllegalArgumentException] {
      EngineSslUtils.loadServerMaterial(
        conf(engineSslConf() + (ENGINE_SSL_KEYSTORE_PATH.key -> unreadable.toString)))
    }
    Seq(unreadable.toString, "is not readable", user, host, "NodeManager").foreach { part =>
      assert(e2.getMessage.contains(part))
    }
  }

  test("the key store password is resolved from a credential provider alias") {
    val alias = ENGINE_SSL_KEYSTORE_PASSWORD.key -> passwordOf(goodKeyStore)
    val providerPath = createCredentialProvider(workDir, alias)
    val settings = engineSslConf() - ENGINE_SSL_KEYSTORE_PASSWORD.key +
      (CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH -> providerPath)
    val material = EngineSslUtils.loadServerMaterial(conf(settings))
    assert(material.keyManagerFactory.getKeyManagers.nonEmpty)
  }

  test("a protocol or cipher suite policy that leaves nothing enabled fails the loading") {
    val noProtocol = intercept[IllegalArgumentException] {
      EngineSslUtils.loadServerMaterial(conf(engineSslConf() +
        (ENGINE_SSL_DISALLOWED_PROTOCOLS.key -> "SSLv3,TLSv1,TLSv1.1,TLSv1.2,TLSv1.3")))
    }
    assert(noProtocol.getMessage.contains(ENGINE_SSL_DISALLOWED_PROTOCOLS.key))

    val noCipher = intercept[IllegalArgumentException] {
      EngineSslUtils.loadServerMaterial(conf(engineSslConf() +
        (ENGINE_SSL_INCLUDE_CIPHER_SUITES.key -> "TLS_NOT_A_CIPHER_SUITE")))
    }
    assert(noCipher.getMessage.contains(ENGINE_SSL_INCLUDE_CIPHER_SUITES.key))
  }

  test("enabled protocols and cipher suites") {
    assert(EngineSslUtils.enabledProtocols(Array("TLSv1.2", "TLSv1.3"), Set("tlsv1.2")) ===
      Array("TLSv1.3"))
    assert(EngineSslUtils.enabledProtocols(Array("TLSv1.2", "TLSv1.3"), Set.empty) ===
      Array("TLSv1.2", "TLSv1.3"))
    intercept[IllegalArgumentException] {
      EngineSslUtils.enabledProtocols(Array("TLSv1.2", "TLSv1.3"), Set("TLSV1.2", "tlsv1.3"))
    }

    assert(EngineSslUtils.enabledCipherSuites(Array("A", "B", "C"), Seq("C", "X", " A")) ===
      Array("C", "A"))
    intercept[IllegalArgumentException] {
      EngineSslUtils.enabledCipherSuites(Array("A", "B"), Seq("X"))
    }

    val suite = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
    val material = EngineSslUtils.loadServerMaterial(conf(engineSslConf() +
      (ENGINE_SSL_INCLUDE_CIPHER_SUITES.key -> s"$suite,TLS_NOT_A_CIPHER_SUITE") +
      (ENGINE_SSL_DISALLOWED_PROTOCOLS.key -> "SSLv3,TLSv1,TLSv1.1,TLSv1.3")))
    // the policy that the Spark Connect listener gets as well
    assert(material.cipherSuites === Seq(suite))
    assert(material.protocols.contains("TLSv1.2"))
    assert(material.protocols.intersect(Seq("SSLv3", "TLSv1", "TLSv1.1", "TLSv1.3")).isEmpty)
    val serverSocket = EngineSslUtils.newServerSocket(material, 0, InetAddress.getLoopbackAddress)
      .getServerSocket.asInstanceOf[SSLServerSocket]
    try {
      assert(serverSocket.getEnabledCipherSuites.toSeq === material.cipherSuites)
      assert(serverSocket.getEnabledProtocols.toSeq === material.protocols)
    } finally {
      serverSocket.close()
    }
  }

  test("client context with the JVM default or a custom trust store") {
    val jvmDefault = EngineSslUtils.loadClientContext(conf(Map.empty))
    assert(jvmDefault.trustSource === EngineSslUtils.JVM_DEFAULT_TRUST_SOURCE)
    assert(jvmDefault.trustManagers.nonEmpty)
    assert(jvmDefault.expectedHostname.isEmpty)

    val custom = EngineSslUtils.loadClientContext(conf(serverSslConf(jksTrustStore) +
      (ENGINE_SSL_EXPECTED_HOSTNAME.key -> "engine.example.com")))
    assert(custom.trustSource === jksTrustStore.toString)
    assert(custom.expectedHostname === Some("engine.example.com"))

    // the CA of the test material is not in the JVM default trust store
    withTlsServer(goodKeyStore) { port =>
      val e = connectFailure(port, ctx = jvmDefault)
      assert(e.getMessage.contains(s"not trusted by ${EngineSslUtils.JVM_DEFAULT_TRUST_SOURCE}"))
    }
  }

  test("an invalid trust store fails the client context") {
    val missing = workDir.resolve("missing-truststore.p12").toString
    val e1 = intercept[IllegalArgumentException] {
      EngineSslUtils.loadClientContext(
        conf(serverSslConf() + (ENGINE_SSL_TRUSTSTORE_PATH.key -> missing)))
    }
    assert(e1.getMessage.contains(ENGINE_SSL_TRUSTSTORE_PATH.key))

    val e2 = intercept[IllegalArgumentException] {
      EngineSslUtils.loadClientContext(
        conf(serverSslConf() + (ENGINE_SSL_TRUSTSTORE_PASSWORD.key -> "not-the-password")))
    }
    assert(e2.getMessage.contains(ENGINE_SSL_TRUSTSTORE_PASSWORD.key))

    val empty = generateTrustStore(workDir)
    val e3 = intercept[IllegalArgumentException] {
      EngineSslUtils.loadClientContext(conf(serverSslConf(empty)))
    }
    assert(e3.getMessage.contains("contains no readable certificate"))
  }

  test("the handshake succeeds and the socket carries the timeout and max message size") {
    val socketTimeoutMs = 12345
    withTlsServer(goodKeyStore) { port =>
      Seq("localhost", "127.0.0.1").foreach { host =>
        val socket = EngineSslUtils.openClientSocket(
          clientContext,
          host,
          port,
          socketTimeoutMs,
          timeoutMs,
          maxMessageSize)
        try {
          assert(socket.isOpen)
          assert(socket.getConfiguration.getMaxMessageSize === maxMessageSize)
          val ssl = socket.getSocket.asInstanceOf[SSLSocket]
          assert(Set("TLSv1.2", "TLSv1.3").contains(ssl.getSession.getProtocol))
          assert(ssl.getSoTimeout === socketTimeoutMs)
        } finally {
          socket.close()
        }
      }
    }
  }

  test("a plaintext peer is reported as not speaking TLS") {
    val plain = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    val responder = new Thread(
      new Runnable {
        override def run(): Unit = Try(plain.accept()).foreach { socket =>
          try {
            socket.setSoTimeout(timeoutMs)
            val in = socket.getInputStream
            // answer the ClientHello, and drain the input before closing so that the client
            // reads the answer instead of a connection reset
            in.read(new Array[Byte](1024))
            socket.getOutputStream.write(
              "HTTP/1.1 400 Bad Request\r\n\r\n".getBytes(StandardCharsets.US_ASCII))
            socket.getOutputStream.flush()
            socket.shutdownOutput()
            while (in.read() >= 0) {}
          } catch {
            case _: IOException =>
          } finally {
            socket.close()
          }
        }
      },
      "engine-ssl-utils-suite-plaintext-peer")
    responder.setDaemon(true)
    responder.start()
    try {
      val e = connectFailure(plain.getLocalPort)
      assert(e.getMessage.contains("not speaking TLS"))
      assert(e.getMessage.contains(ENGINE_SSL_ENABLED.key))
    } finally {
      plain.close()
      responder.join(timeoutMs)
    }
  }

  test("an untrusted engine certificate is rejected") {
    withTlsServer(foreignKeyStore) { port =>
      val e = connectFailure(port)
      assert(e.getMessage.contains(s"not trusted by ${trustStore.toString}"))
      assert(e.getMessage.contains(ENGINE_SSL_TRUSTSTORE_PATH.key))
    }
  }

  test("the engine host name is always verified, also on loopback") {
    withTlsServer(otherHostKeyStore) { port =>
      val e = connectFailure(port)
      assert(e.getMessage.contains("does not match localhost"))
      assert(e.getMessage.contains(ENGINE_SSL_EXPECTED_HOSTNAME.key))

      val expected = EngineSslUtils.loadClientContext(
        conf(serverSslConf() + (ENGINE_SSL_EXPECTED_HOSTNAME.key -> "other.invalid")))
      val socket = connect(port, ctx = expected)
      socket.close()
    }

    withTlsServer(localhostOnlyKeyStore) { port =>
      connect(port, "localhost").close()
      val e = connectFailure(port, "127.0.0.1")
      assert(e.getMessage.contains("does not match 127.0.0.1"))
    }
  }

  test("an expired engine certificate is rejected") {
    withTlsServer(expiredKeyStore) { port =>
      val e = connectFailure(port)
      assert(e.getMessage.contains("expired"))
    }
  }

  test("a silent peer fails within the connect timeout") {
    // the kernel completes the TCP handshake for the backlog, nobody ever answers
    val silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    try {
      val connectTimeoutMs = 2000
      val start = System.nanoTime()
      val e = intercept[TTransportException] {
        connect(silent.getLocalPort, connectTimeoutMs = connectTimeoutMs)
      }
      val elapsedMs = (System.nanoTime() - start) / 1000000
      assert(e.getMessage.contains(s"timed out after $connectTimeoutMs ms"))
      assert(elapsedMs < 2 * connectTimeoutMs)
    } finally {
      silent.close()
    }
  }

  test("a closed port keeps the 'Connection refused' cause") {
    val port = {
      val s = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
      try s.getLocalPort
      finally s.close()
    }
    val e = intercept[TTransportException](connect(port, "127.0.0.1"))
    assert(e.getType === TTransportException.NOT_OPEN)
    assert(e.getCause.isInstanceOf[ConnectException])
    assert(e.getCause.getMessage.contains("Connection refused"))
  }

  test("describe handshake failures") {
    val ctx = new EngineSslClientContext(clientContext.trustManagers, None, "/path/ts.p12")
    def describe(e: Throwable): String =
      EngineSslUtils.describeHandshakeFailure(e, "engine-host", 1234, ctx, 15000)

    assert(describe(new SocketTimeoutException("Read timed out"))
      .contains("timed out after 15000 ms"))
    assert(describe(new IOException(new CertificateExpiredException("x"))).contains("expired"))
    assert(describe(new IOException(new CertificateException(
      "No subject alternative DNS name matching engine-host found.")))
      .contains("does not match engine-host"))
    assert(describe(new IOException("PKIX path building failed"))
      .contains("not trusted by /path/ts.p12"))
    assert(describe(new IOException(new EOFException("SSL peer shut down")))
      .contains("not speaking TLS"))
    assert(describe(new IOException("something else")) ===
      "TLS handshake with engine engine-host:1234 failed: something else")
  }
}
