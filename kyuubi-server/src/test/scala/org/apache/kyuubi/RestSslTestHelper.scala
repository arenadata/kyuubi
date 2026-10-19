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

package org.apache.kyuubi

import java.io.{File, InputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.security.cert.X509Certificate
import javax.net.ssl.{HttpsURLConnection, SSLContext, SSLSocket, TrustManager, X509TrustManager}

import scala.io.Source

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

/**
 * Utilities for testing the RESTful frontend service with SSL enabled.
 */
object RestSslTestHelper {

  val KEYSTORE_PASSWORD: String = KeyStoreTestUtils.PASSWORD

  private val TIMEOUT_MS = 30000

  /** The host name used by containers to reach the services running on the test host. */
  val CONTAINER_HOST_ALIAS = "host.testcontainers.internal"

  /**
   * Generate a keystore with a self-signed certificate valid for the local host and for
   * the host alias used by Testcontainers.
   */
  def generateKeyStore(dir: File, storeType: String = "JKS"): File =
    KeyStoreTestUtils.generateKeyStore(
      dir.toPath,
      alias = "kyuubi",
      sans = Seq("dns:localhost", s"dns:$CONTAINER_HOST_ALIAS", "ip:127.0.0.1"),
      storeType = storeType).toFile

  /** Export the certificate of the keystore in PEM format, to be trusted by external clients. */
  def exportCertificate(keyStore: File): File = KeyStoreTestUtils.exportPem(keyStore.toPath).toFile

  /** Generate a truststore with the certificate of the keystore, for the Kyuubi clients. */
  def generateTrustStore(keyStore: File, storeType: String = "JKS"): File =
    KeyStoreTestUtils.generateTrustStore(keyStore.toPath.getParent, storeType, keyStore.toPath)
      .toFile

  /**
   * Create a JCEKS credential provider holding the given aliases, and return the path to set as
   * hadoop.security.credential.provider.path.
   */
  def createCredentialProvider(dir: File, aliases: (String, String)*): String =
    KeyStoreTestUtils.createCredentialProvider(dir.toPath, aliases: _*)

  /** Settings enabling SSL for the RESTful frontend service with the given keystore. */
  def sslConf(conf: KyuubiConf, keyStore: File): KyuubiConf = conf
    .set(FRONTEND_REST_SSL_ENABLED, true)
    .set(FRONTEND_REST_SSL_KEYSTORE_PATH, keyStore.getAbsolutePath)
    .set(FRONTEND_REST_SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD)

  lazy val trustAllSslContext: SSLContext = {
    val trustAll = new X509TrustManager {
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      override def getAcceptedIssuers: Array[X509Certificate] = Array.empty
    }
    val context = SSLContext.getInstance("TLS")
    context.init(null, Array[TrustManager](trustAll), null)
    context
  }

  def httpsConnection(hostAndPort: String, path: String): HttpsURLConnection = {
    val conn = new URL(s"https://$hostAndPort$path").openConnection()
      .asInstanceOf[HttpsURLConnection]
    conn.setSSLSocketFactory(trustAllSslContext.getSocketFactory)
    conn.setHostnameVerifier((_, _) => true)
    conn.setInstanceFollowRedirects(false)
    conn.setConnectTimeout(TIMEOUT_MS)
    conn.setReadTimeout(TIMEOUT_MS)
    conn
  }

  def readBody(conn: HttpURLConnection): String = read(conn.getInputStream)

  def readErrorBody(conn: HttpURLConnection): String = read(conn.getErrorStream)

  /** Open a TLS socket to the given host:port without starting the handshake. */
  def sslSocket(hostAndPort: String): SSLSocket = {
    val separator = hostAndPort.lastIndexOf(':')
    val socket = trustAllSslContext.getSocketFactory.createSocket(
      hostAndPort.substring(0, separator),
      hostAndPort.substring(separator + 1).toInt).asInstanceOf[SSLSocket]
    socket.setSoTimeout(TIMEOUT_MS)
    socket
  }

  private def read(in: InputStream): String = {
    val source = Source.fromInputStream(in, StandardCharsets.UTF_8.name())
    try source.mkString
    finally source.close()
  }
}
