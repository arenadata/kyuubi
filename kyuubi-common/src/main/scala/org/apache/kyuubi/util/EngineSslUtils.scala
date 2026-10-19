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
import java.net.{InetAddress, InetSocketAddress, Socket, SocketTimeoutException}
import java.nio.file.{Files, Paths}
import java.security.{GeneralSecurityException, KeyStore}
import java.security.cert.{CertificateExpiredException, CertificateNotYetValidException, CertPathBuilderException, CertPathValidatorException}
import java.util.Locale
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLServerSocket, SSLSocket, SSLSocketFactory, TrustManager, TrustManagerFactory}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.shaded.thrift.transport.{TServerSocket, TSocket, TTransportException}

/**
 * Key material and TLS policy of the listeners of an engine, loaded once. The Thrift and the
 * Spark Connect listener serve the same certificate with the same protocols and cipher suites.
 *
 * @param keyManagerFactory initialized with the engine key store
 * @param sslContext the context of the Thrift listener, initialized with the same key managers
 * @param protocols the TLS protocol versions the listeners enable
 * @param cipherSuites the cipher suites the listeners enable; when empty, each listener keeps the
 *                     defaults of its TLS stack
 */
final class EngineSslServerMaterial private[util] (
    val keyManagerFactory: KeyManagerFactory,
    val sslContext: SSLContext,
    val protocols: Seq[String],
    val cipherSuites: Seq[String])

/**
 * Trust material the Kyuubi server uses to open TLS connections to engines.
 *
 * @param trustManagers verify the engine certificates, also reused by the gRPC channel
 * @param expectedHostname the name engine certificates are verified against instead of the
 *                         host the engine registers
 * @param trustSource the trust store path, or a description of the JVM default trust store,
 *                    only used in messages
 */
final class EngineSslClientContext(
    val trustManagers: Array[TrustManager],
    val expectedHostname: Option[String],
    val trustSource: String) {

  // built once, so that TLS sessions can be resumed across engine connections
  val sslContext: SSLContext = {
    val context = SSLContext.getInstance("TLS")
    context.init(null, trustManagers, null)
    context
  }

  def socketFactory: SSLSocketFactory = sslContext.getSocketFactory
}

/**
 * TLS for the Thrift binary connections between the Kyuubi server and its engines. Only JDK
 * and relocated Thrift types are used, so engines can shade it without new dependencies.
 */
object EngineSslUtils extends Logging {

  val JVM_DEFAULT_TRUST_SOURCE = "JVM default trust store"

  /**
   * Load the key material of an engine's TLS listeners and apply the protocol and cipher suite
   * policy to it. Error messages name the config key to fix and never contain the password.
   */
  def loadServerMaterial(conf: KyuubiConf): EngineSslServerMaterial = {
    val path = conf.get(ENGINE_SSL_KEYSTORE_PATH).getOrElse {
      throw new IllegalArgumentException(
        s"${ENGINE_SSL_KEYSTORE_PATH.key} not configured, it is required when " +
          s"${ENGINE_SSL_ENABLED.key} is true")
    }
    val password = KyuubiHadoopUtils.getPassword(conf, ENGINE_SSL_KEYSTORE_PASSWORD).getOrElse {
      throw new IllegalArgumentException(
        s"${ENGINE_SSL_KEYSTORE_PASSWORD.key} not configured. The Kyuubi server does not " +
          "forward it to engines: provide it as a Hadoop credential provider alias named " +
          s"${ENGINE_SSL_KEYSTORE_PASSWORD.key}, or set " +
          s"spark.${ENGINE_SSL_KEYSTORE_PASSWORD.key} in the engine-side spark-defaults.conf")
    }
    val storeType = conf.get(ENGINE_SSL_KEYSTORE_TYPE).getOrElse(KeyStore.getDefaultType)
    val algorithm =
      conf.get(ENGINE_SSL_KEYSTORE_ALGORITHM).getOrElse(KeyManagerFactory.getDefaultAlgorithm)

    val keyStore = loadStore(
      path,
      ENGINE_SSL_KEYSTORE_PATH.key,
      storeType,
      ENGINE_SSL_KEYSTORE_TYPE.key,
      Some(password),
      ENGINE_SSL_KEYSTORE_PASSWORD.key,
      " In YARN cluster mode the key store must exist on every NodeManager and be readable" +
        " by the container user.")
    if (!keyStore.aliases().asScala.exists(keyStore.isKeyEntry)) {
      throw new IllegalArgumentException(
        s"Key store $path (${ENGINE_SSL_KEYSTORE_PATH.key}) contains no private key entry")
    }
    val kmf =
      try KeyManagerFactory.getInstance(algorithm)
      catch {
        case e: GeneralSecurityException =>
          throw new IllegalArgumentException(
            s"Unsupported key manager algorithm $algorithm (${ENGINE_SSL_KEYSTORE_ALGORITHM.key})",
            e)
      }
    try kmf.init(keyStore, password.toCharArray)
    catch {
      case e: GeneralSecurityException =>
        throw new IllegalArgumentException(
          s"Cannot recover the private key from key store $path; check " +
            s"${ENGINE_SSL_KEYSTORE_PASSWORD.key}, which is also used as the key password",
          e)
    }

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(kmf.getKeyManagers, null, null)
    // the protocol and cipher suite policy is resolved once, against an unbound socket, so that
    // a policy that leaves nothing enabled fails here, before any listener exists
    val probe =
      sslContext.getServerSocketFactory.createServerSocket().asInstanceOf[SSLServerSocket]
    try {
      val protocols =
        enabledProtocols(probe.getEnabledProtocols, conf.get(ENGINE_SSL_DISALLOWED_PROTOCOLS))
      val includeCipherSuites = conf.get(ENGINE_SSL_INCLUDE_CIPHER_SUITES)
      val cipherSuites =
        if (includeCipherSuites.nonEmpty) {
          enabledCipherSuites(probe.getSupportedCipherSuites, includeCipherSuites)
        } else {
          Array.empty[String]
        }
      new EngineSslServerMaterial(kmf, sslContext, protocols.toSeq, cipherSuites.toSeq)
    } finally {
      probe.close()
    }
  }

  /**
   * Load the trust material the Kyuubi server uses to verify engine certificates. Without a
   * trust store path, the JVM default trust store is used.
   */
  def loadClientContext(serverConf: KyuubiConf): EngineSslClientContext = {
    val expectedHostname = serverConf.get(ENGINE_SSL_EXPECTED_HOSTNAME).map(_.trim)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    serverConf.get(ENGINE_SSL_TRUSTSTORE_PATH) match {
      case Some(path) =>
        val trustStore = loadStore(
          path,
          ENGINE_SSL_TRUSTSTORE_PATH.key,
          serverConf.get(ENGINE_SSL_TRUSTSTORE_TYPE).getOrElse(KeyStore.getDefaultType),
          ENGINE_SSL_TRUSTSTORE_TYPE.key,
          KyuubiHadoopUtils.getPassword(serverConf, ENGINE_SSL_TRUSTSTORE_PASSWORD),
          ENGINE_SSL_TRUSTSTORE_PASSWORD.key,
          "")
        // an empty store would only fail at the first handshake, with a misleading message
        if (!trustStore.aliases().asScala.exists(a => trustStore.getCertificate(a) != null)) {
          throw new IllegalArgumentException(
            s"Trust store $path (${ENGINE_SSL_TRUSTSTORE_PATH.key}) contains no readable " +
              s"certificate; a PKCS12 trust store needs ${ENGINE_SSL_TRUSTSTORE_PASSWORD.key}")
        }
        tmf.init(trustStore)
        new EngineSslClientContext(tmf.getTrustManagers, expectedHostname, path)
      case None =>
        tmf.init(null: KeyStore)
        new EngineSslClientContext(tmf.getTrustManagers, expectedHostname, JVM_DEFAULT_TRUST_SOURCE)
    }
  }

  /** Create the TLS listener of an engine's Thrift binary frontend. */
  def newServerSocket(
      m: EngineSslServerMaterial,
      port: Int,
      bindAddr: InetAddress): TServerSocket = {
    val serverSocket = m.sslContext.getServerSocketFactory
      .createServerSocket(port, -1, bindAddr).asInstanceOf[SSLServerSocket]
    try {
      configure(serverSocket, m)
      info("Engine TLS listener enabled protocols: " +
        serverSocket.getEnabledProtocols.mkString(","))
      new TServerSocket(serverSocket)
    } catch {
      case e: Throwable =>
        Try(serverSocket.close())
        throw e
    }
  }

  private def configure(serverSocket: SSLServerSocket, m: EngineSslServerMaterial): Unit = {
    serverSocket.setEnabledProtocols(m.protocols.toArray)
    if (m.cipherSuites.nonEmpty) {
      serverSocket.setEnabledCipherSuites(m.cipherSuites.toArray)
    }
  }

  /** The protocols left after removing the disallowed ones, ignoring case. */
  def enabledProtocols(enabled: Array[String], disallowed: Set[String]): Array[String] = {
    val lowerDisallowed = disallowed.map(_.toLowerCase(Locale.ROOT))
    val remaining = enabled.filterNot(p => lowerDisallowed.contains(p.toLowerCase(Locale.ROOT)))
    if (remaining.isEmpty) {
      throw new IllegalArgumentException(
        s"No TLS protocol remains enabled after applying ${ENGINE_SSL_DISALLOWED_PROTOCOLS.key}=" +
          s"${disallowed.mkString(",")} to the protocols enabled by the JVM: " +
          enabled.mkString(","))
    }
    remaining
  }

  /** The included cipher suites that the JVM supports, in the configured order. */
  def enabledCipherSuites(supported: Array[String], include: Seq[String]): Array[String] = {
    val supportedSet = supported.toSet
    val remaining = include.map(_.trim).filter(supportedSet.contains).distinct.toArray
    if (remaining.isEmpty) {
      throw new IllegalArgumentException(
        s"None of the cipher suites in ${ENGINE_SSL_INCLUDE_CIPHER_SUITES.key}=" +
          s"${include.mkString(",")} is supported by the JVM")
    }
    remaining
  }

  /**
   * Open a TLS connection to an engine and complete the handshake, verifying the engine
   * certificate chain and its host name. The returned socket is open, so a SASL transport
   * wrapped around it does not open it again.
   *
   * @param socketTimeoutMs the read timeout once the connection is established, 0 for none
   * @param connectTimeoutMs bounds both the TCP connect and the TLS handshake
   */
  def openClientSocket(
      ctx: EngineSslClientContext,
      host: String,
      port: Int,
      socketTimeoutMs: Int,
      connectTimeoutMs: Int,
      maxMessageSize: Int): TSocket = {
    val raw = new Socket()
    try {
      raw.connect(new InetSocketAddress(host, port), connectTimeoutMs)
      raw.setSoTimeout(connectTimeoutMs)
    } catch {
      // the ConnectException cause is kept, the server retries on 'Connection refused'
      case e: IOException =>
        Try(raw.close())
        throw new TTransportException(TTransportException.NOT_OPEN, e)
    }

    val ssl =
      try {
        val socket = ctx.socketFactory
          .createSocket(raw, ctx.expectedHostname.getOrElse(host), port, true)
          .asInstanceOf[SSLSocket]
        val params = socket.getSSLParameters
        params.setEndpointIdentificationAlgorithm("HTTPS")
        socket.setSSLParameters(params)
        socket.startHandshake()
        socket.setSoTimeout(socketTimeoutMs)
        socket
      } catch {
        case e: IOException =>
          Try(raw.close())
          throw new TTransportException(
            TTransportException.NOT_OPEN,
            describeHandshakeFailure(e, host, port, ctx, connectTimeoutMs),
            e)
        case e: Throwable =>
          Try(raw.close())
          throw e
      }

    try {
      val tSocket = new TSocket(ssl)
      // the relocated Thrift only has TSocket(Socket), which takes a default TConfiguration
      tSocket.getConfiguration.setMaxMessageSize(maxMessageSize)
      tSocket
    } catch {
      case e: Throwable =>
        Try(ssl.close())
        throw e
    }
  }

  /** A message that names the likely cause of a failed TLS handshake and the key to fix. */
  def describeHandshakeFailure(
      e: Throwable,
      host: String,
      port: Int,
      ctx: EngineSslClientContext,
      handshakeTimeoutMs: Int = 0): String = {
    val chain = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16).toList
    def has(cls: Class[_]): Boolean = chain.exists(cls.isInstance)
    def mentions(text: String): Boolean =
      chain.exists(t => Option(t.getMessage).exists(_.contains(text)))

    val reason =
      if (has(classOf[SocketTimeoutException])) {
        val after = if (handshakeTimeoutMs > 0) s" after $handshakeTimeoutMs ms" else ""
        s"the TLS handshake timed out$after (${ENGINE_LOGIN_TIMEOUT.key})"
      } else if (has(classOf[CertificateExpiredException])) {
        "the engine certificate has expired"
      } else if (has(classOf[CertificateNotYetValidException])) {
        "the engine certificate is not yet valid"
      } else if (mentions("No subject alternative") || mentions("No name matching")) {
        s"the engine certificate does not match ${ctx.expectedHostname.getOrElse(host)}; add " +
          "it to the subject alternative names of the engine certificate or set " +
          ENGINE_SSL_EXPECTED_HOSTNAME.key
      } else if (has(classOf[CertPathBuilderException]) ||
        has(classOf[CertPathValidatorException]) || mentions("PKIX")) {
        s"the engine certificate is not trusted by ${ctx.trustSource} " +
          s"(${ENGINE_SSL_TRUSTSTORE_PATH.key})"
      } else if (has(classOf[EOFException]) ||
        mentions("Unsupported or unrecognized SSL message") ||
        mentions("Remote host terminated") || mentions("Remote host closed")) {
        "the peer is not speaking TLS; the engine may have been started with " +
          s"${ENGINE_SSL_ENABLED.key}=false"
      } else {
        Option(e.getMessage).getOrElse(e.getClass.getName)
      }
    s"TLS handshake with engine $host:$port failed: $reason"
  }

  private def loadStore(
      path: String,
      pathKey: String,
      storeType: String,
      typeKey: String,
      password: Option[String],
      passwordKey: String,
      hint: String): KeyStore = {
    val file = Paths.get(path)
    val problem =
      if (!Files.exists(file)) Some("does not exist")
      else if (!Files.isRegularFile(file)) Some("is not a regular file")
      else if (!Files.isReadable(file)) Some("is not readable")
      else None
    problem.foreach { p =>
      throw new IllegalArgumentException(
        s"$path ($pathKey) $p for user ${System.getProperty("user.name")} on host " +
          s"$localHostName.$hint")
    }

    val store =
      try KeyStore.getInstance(storeType)
      catch {
        case e: GeneralSecurityException =>
          throw new IllegalArgumentException(s"Unsupported key store type $storeType ($typeKey)", e)
      }
    val in = Files.newInputStream(file)
    try store.load(in, password.map(_.toCharArray).orNull)
    catch {
      // JDK messages here never contain the password
      case e @ (_: IOException | _: GeneralSecurityException) =>
        throw new IllegalArgumentException(
          s"Failed to load $path ($pathKey) as a $storeType key store, check $passwordKey and " +
            s"$typeKey: ${e.getMessage}",
          e)
    } finally {
      in.close()
    }
    store
  }

  private def localHostName: String =
    Try(InetAddress.getLocalHost.getHostName).getOrElse("<unknown>")
}
