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

package org.apache.kyuubi.server

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util.UUID
import javax.net.ssl.{SSLContext, SSLHandshakeException}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.util.Try

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.handler.{AbstractHandler, ContextHandler}
import org.eclipse.jetty.util.ssl.SslContextFactory

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper, Utils}
import org.apache.kyuubi.RestSslTestHelper._
import org.apache.kyuubi.client.{BaseRestApi, KyuubiRestClient}
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ctl.CtlConf._
import org.apache.kyuubi.ctl.cli.ControlCli
import org.apache.kyuubi.ctl.cmd.log.LogBatchCommand
import org.apache.kyuubi.ctl.opt.{BatchOpts, CliConfig, CommonOpts}
import org.apache.kyuubi.server.api.v1.InternalRestClient
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.server.ui.JettyServer
import org.apache.kyuubi.session.{KyuubiSessionManager, SessionType}

class KyuubiRestFrontendServiceSslSuite extends RestFrontendTestHelper {

  private lazy val keyStore = generateKeyStore(Utils.createTempDir().toFile)
  private lazy val trustStore = generateTrustStore(keyStore)
  // a certificate not trusted by the truststore
  private lazy val otherKeyStore = generateKeyStore(Utils.createTempDir().toFile)
  private lazy val otherTrustStore = generateTrustStore(otherKeyStore)

  override protected lazy val conf: KyuubiConf = sslConf(
    KyuubiConf()
      .set(AUTHENTICATION_METHOD, Seq("NONE"))
      .set(FRONTEND_REST_BIND_HOST.key, "localhost")
      .set(BATCH_SUBMITTER_ENABLED, true)
      .set(BATCH_INTERNAL_REST_CLIENT_REQUEST_MAX_ATTEMPTS, 1)
      .set(BATCH_INTERNAL_REST_CLIENT_REQUEST_ATTEMPT_WAIT, 100L)
      .set(BATCH_INTERNAL_REST_CLIENT_SSL_TRUSTSTORE_PATH, trustStore.getAbsolutePath)
      .set(BATCH_INTERNAL_REST_CLIENT_SSL_TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD),
    keyStore)

  private def restFe: KyuubiRestFrontendService = fe.asInstanceOf[KyuubiRestFrontendService]

  private def sessionManager: KyuubiSessionManager =
    restFe.be.sessionManager.asInstanceOf[KyuubiSessionManager]

  private def withBatchMetadata[T](
      kyuubiInstance: String,
      requestConf: Map[String, String] = Map.empty)(f: Metadata => T): T = {
    val metadata = Metadata(
      identifier = UUID.randomUUID().toString,
      sessionType = SessionType.BATCH,
      realUser = "anonymous",
      username = "anonymous",
      ipAddress = "localhost",
      kyuubiInstance = kyuubiInstance,
      state = "PENDING",
      resource = "resource",
      className = "className",
      requestName = "rest-ssl",
      requestConf = requestConf,
      engineType = "SPARK")
    sessionManager.insertMetadata(metadata)
    try {
      f(metadata)
    } finally {
      sessionManager.cleanupMetadata(metadata.identifier)
    }
  }

  private def newInternalRestClient(
      scheme: String,
      trustStore: Option[File] = None): InternalRestClient = new InternalRestClient(
    fe.connectionUrl,
    scheme,
    conf.get(FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER),
    socketTimeout = 10000,
    connectTimeout = 10000,
    securityEnabled = false,
    requestMaxAttempts = 1,
    requestAttemptWait = 100,
    trustStorePath = trustStore.map(_.getAbsolutePath),
    trustStorePassword = trustStore.map(_ => KEYSTORE_PASSWORD))

  private def hasSslHandshakeFailure(e: Throwable): Boolean =
    ExceptionUtils.indexOfType(e, classOf[SSLHandshakeException]) >= 0

  /** A fake kyuubi instance serving the given batch local log over https. */
  private def withHttpsInstance[T](keyStore: File, log: String)(f: String => T): T = {
    val sslContextFactory = new SslContextFactory.Server
    sslContextFactory.setKeyStorePath(keyStore.getAbsolutePath)
    sslContextFactory.setKeyStorePassword(KEYSTORE_PASSWORD)
    val server =
      JettyServer("https-instance", "localhost", 0, 100, 1000L, false, Some(sslContextFactory))
    val handler = new ContextHandler("/")
    handler.setHandler(new AbstractHandler {
      override def handle(
          target: String,
          baseRequest: Request,
          request: HttpServletRequest,
          response: HttpServletResponse): Unit = {
        response.setContentType("application/json")
        response.getWriter.print(s"""{"logRowSet":["$log"],"rowCount":1}""")
        baseRequest.setHandled(true)
      }
    })
    server.addHandler(handler)
    server.start()
    try {
      f(server.getServerUri)
    } finally {
      server.stop()
    }
  }

  /** Run the kyuubi-ctl command and return what it prints. */
  private def runCtlCommand(command: => LogBatchCommand): String = {
    // kyuubi-ctl prints the batch logs to the print stream of the control cli
    val output = new ByteArrayOutputStream()
    val originalPrintStream = ControlCli.printStream
    ControlCli.printStream = new PrintStream(output, true, StandardCharsets.UTF_8.name())
    try {
      command.doRun()
      output.toString(StandardCharsets.UTF_8.name())
    } finally {
      ControlCli.printStream = originalPrintStream
    }
  }

  test("connection scheme is https") {
    assert(restFe.connectionScheme === "https")
  }

  test("REST API is served over https") {
    val conn = httpsConnection(fe.connectionUrl, "/api/v1/ping")
    assert(conn.getResponseCode === 200)
    assert(readBody(conn) === "pong")
  }

  test("Web UI is served over https") {
    val redirect = httpsConnection(fe.connectionUrl, "/")
    assert(redirect.getResponseCode === 302)
    assert(redirect.getHeaderField("Location") === s"https://${fe.connectionUrl}/ui")

    val ui = httpsConnection(fe.connectionUrl, "/ui/")
    assert(ui.getResponseCode === 200)
    assert(readBody(ui).contains("<html"))
  }

  test("OpenAPI server url uses https scheme") {
    val conn = httpsConnection(fe.connectionUrl, "/api/openapi.json")
    assert(conn.getResponseCode === 200)
    assert(readBody(conn).contains(s"https://${fe.connectionUrl}/api"))
  }

  test("plain http is not served") {
    val conn = new URL(s"http://${fe.connectionUrl}/api/v1/ping").openConnection()
      .asInstanceOf[HttpURLConnection]
    val responseCode = Try(conn.getResponseCode)
    assert(responseCode.isFailure || responseCode.get != 200)
  }

  test("kyuubi rest client works over https") {
    val client = KyuubiRestClient.builder(s"https://${fe.connectionUrl}")
      .apiVersion(KyuubiRestClient.ApiVersion.V1)
      .username("anonymous")
      .password("anonymous")
      .maxAttempts(1)
      .build()
    try {
      assert(new BaseRestApi(client).ping() === "pong")
    } finally {
      client.close()
    }
  }

  test("internal rest client uses the given scheme") {
    withBatchMetadata(fe.connectionUrl) { metadata =>
      val batch =
        newInternalRestClient("https").getBatch("anonymous", "localhost", metadata.identifier)
      assert(batch.getId === metadata.identifier)
      assert(batch.getKyuubiInstance === fe.connectionUrl)

      val e = intercept[KyuubiRestException] {
        newInternalRestClient("http").getBatch("anonymous", "localhost", metadata.identifier)
      }
      assert(e.getMessage.contains(s"http://${fe.connectionUrl}"))
    }
  }

  test("batch local log is redirected to another kyuubi instance over https") {
    // nothing listens on the other kyuubi instance
    val otherInstance = "127.0.0.1:1"
    withBatchMetadata(otherInstance) { metadata =>
      val conn =
        httpsConnection(fe.connectionUrl, s"/api/v1/batches/${metadata.identifier}/localLog")
      assert(conn.getResponseCode === 500)
      assert(readErrorBody(conn).contains(s"Api request failed for https://$otherInstance"))
    }
  }

  test("kyuubi-ctl fetches batch logs via the rest base url if the instance is unreachable") {
    // the batch is owned by this kyuubi instance, while the batch info given to the command
    // points to a plain http server, which does not accept https requests
    val plainHttpServer = JettyServer("plain-http-instance", "localhost", 0, 100, 1000L, false)
    plainHttpServer.start()
    try {
      withBatchMetadata(fe.connectionUrl, Map(BATCH_IMPL_VERSION.key -> "2")) { metadata =>
        val cliConfig = CliConfig(
          commonOpts = CommonOpts(
            hostUrl = s"https://${fe.connectionUrl}",
            authSchema = "basic",
            username = "anonymous",
            password = "anonymous"),
          batchOpts = BatchOpts(batchId = metadata.identifier, size = 10))
        val batch = new Batch()
        batch.setId(metadata.identifier)
        batch.setKyuubiInstance(plainHttpServer.getServerUri)
        val expectedLog = s"Batch ${metadata.identifier} is waiting for submitting"
        // the instance or the base url is chosen randomly as the first host url to request
        (1 to 5).foreach { _ =>
          val printed = runCtlCommand(new LogBatchCommand(cliConfig, Some(batch)))
          assert(printed.split(System.lineSeparator()).contains(expectedLog), printed)
        }
      }
    } finally {
      plainHttpServer.stop()
    }
  }

  test("internal rest client verifies the server certificate with the truststore") {
    withBatchMetadata(fe.connectionUrl) { metadata =>
      val batch = newInternalRestClient("https", Some(trustStore))
        .getBatch("anonymous", "localhost", metadata.identifier)
      assert(batch.getId === metadata.identifier)

      val e = intercept[KyuubiRestException] {
        newInternalRestClient("https", Some(otherTrustStore))
          .getBatch("anonymous", "localhost", metadata.identifier)
      }
      assert(hasSslHandshakeFailure(e), e)
    }
  }

  test("batch local log is redirected to a kyuubi instance trusted by the truststore") {
    val expectedLog = "batch log of the other kyuubi instance"
    withHttpsInstance(keyStore, expectedLog) { otherInstance =>
      withBatchMetadata(otherInstance) { metadata =>
        val conn =
          httpsConnection(fe.connectionUrl, s"/api/v1/batches/${metadata.identifier}/localLog")
        assert(conn.getResponseCode === 200)
        assert(readBody(conn).contains(expectedLog))
      }
    }

    // the certificate of the other kyuubi instance is not in the truststore
    withHttpsInstance(otherKeyStore, expectedLog) { otherInstance =>
      withBatchMetadata(otherInstance) { metadata =>
        val conn =
          httpsConnection(fe.connectionUrl, s"/api/v1/batches/${metadata.identifier}/localLog")
        assert(conn.getResponseCode === 500)
        assert(readErrorBody(conn).contains(s"Api request failed for https://$otherInstance"))
      }
    }
  }

  test("kyuubi-ctl verifies the server certificate with the truststore") {
    withBatchMetadata(fe.connectionUrl, Map(BATCH_IMPL_VERSION.key -> "2")) { metadata =>
      def logBatchCommand(trustStore: File): LogBatchCommand = {
        val cliConfig = CliConfig(
          commonOpts = CommonOpts(
            hostUrl = s"https://${fe.connectionUrl}",
            authSchema = "basic",
            username = "anonymous",
            password = "anonymous"),
          batchOpts = BatchOpts(batchId = metadata.identifier, size = 10),
          conf = Map(
            CTL_REST_CLIENT_SSL_TRUSTSTORE_PATH.key -> trustStore.getAbsolutePath,
            CTL_REST_CLIENT_SSL_TRUSTSTORE_PASSWORD.key -> KEYSTORE_PASSWORD))
        val batch = new Batch()
        batch.setId(metadata.identifier)
        batch.setKyuubiInstance(fe.connectionUrl)
        new LogBatchCommand(cliConfig, Some(batch))
      }

      val expectedLog = s"Batch ${metadata.identifier} is waiting for submitting"
      val printed = runCtlCommand(logBatchCommand(trustStore))
      assert(printed.split(System.lineSeparator()).contains(expectedLog), printed)

      // the command reports the failure instead of throwing it
      val failed = runCtlCommand(logBatchCommand(otherTrustStore))
      assert(!failed.split(System.lineSeparator()).contains(expectedLog), failed)
      assert(failed.contains("Error fetching batch logs"), failed)
    }
  }
}

class KyuubiRestFrontendServiceSslProtocolSuite extends RestFrontendTestHelper {

  private val disallowedCipherSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
  private val allowedCipherSuite = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"

  // the keystore is configured via the shared kyuubi.frontend.ssl.keystore.* settings
  override protected lazy val conf: KyuubiConf = {
    val keyStore = generateKeyStore(Utils.createTempDir().toFile)
    KyuubiConf()
      .set(AUTHENTICATION_METHOD, Seq("NONE"))
      .set(FRONTEND_REST_BIND_HOST.key, "localhost")
      .set(FRONTEND_REST_SSL_ENABLED, true)
      .set(FRONTEND_SSL_KEYSTORE_PATH, keyStore.getAbsolutePath)
      .set(FRONTEND_SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD)
      .set(FRONTEND_REST_SSL_DISALLOWED_PROTOCOLS, Set("SSLv2", "SSLv3", "TLSv1.3"))
      .set(FRONTEND_REST_SSL_EXCLUDE_CIPHER_SUITES, Seq(disallowedCipherSuite))
  }

  private def handshake(protocol: String, cipherSuite: Option[String] = None): Try[String] = {
    val socket = sslSocket(fe.connectionUrl)
    try {
      Try {
        socket.setEnabledProtocols(Array(protocol))
        cipherSuite.foreach(suite => socket.setEnabledCipherSuites(Array(suite)))
        socket.startHandshake()
        socket.getSession.getProtocol
      }
    } finally {
      socket.close()
    }
  }

  test("REST API is served with the shared keystore settings") {
    val conn = httpsConnection(fe.connectionUrl, "/api/v1/ping")
    assert(conn.getResponseCode === 200)
    assert(readBody(conn) === "pong")
  }

  test("allowed protocol is accepted") {
    assert(handshake("TLSv1.2").get === "TLSv1.2")
  }

  test("disallowed protocol is rejected") {
    assume(SSLContext.getDefault.getSupportedSSLParameters.getProtocols.contains("TLSv1.3"))
    assert(handshake("TLSv1.3").isFailure)
  }

  test("excluded cipher suite is rejected") {
    assert(handshake("TLSv1.2", Some(allowedCipherSuite)).isSuccess)
    assert(handshake("TLSv1.2", Some(disallowedCipherSuite)).isFailure)
  }
}

class KyuubiRestFrontendServiceSslConfSuite extends KyuubiFunSuite {

  // a new conf for every call, KyuubiConf.set mutates it
  private def sslEnabledWithoutKeyStoreConf(): KyuubiConf = KyuubiConf()
    .set(FRONTEND_REST_BIND_HOST.key, "localhost")
    .set(FRONTEND_REST_BIND_PORT, 0)
    .set(FRONTEND_REST_SSL_ENABLED, true)

  test("SSL: missing keystore path throws IllegalArgumentException") {
    val e = intercept[IllegalArgumentException] {
      new KyuubiRestFrontendService(null).initialize(sslEnabledWithoutKeyStoreConf())
    }
    assert(e.getMessage.contains(FRONTEND_REST_SSL_KEYSTORE_PATH.key))
  }

  test("SSL: missing keystore password throws IllegalArgumentException") {
    val e = intercept[IllegalArgumentException] {
      new KyuubiRestFrontendService(null).initialize(sslEnabledWithoutKeyStoreConf()
        .set(FRONTEND_REST_SSL_KEYSTORE_PATH, "/some/path/keystore.jks"))
    }
    assert(e.getMessage.contains(FRONTEND_REST_SSL_KEYSTORE_PASSWORD.key))
  }

  test("SSL: keystore falls back to kyuubi.frontend.ssl.keystore.*") {
    val conf = KyuubiConf(loadSysDefault = false)
      .set(FRONTEND_SSL_KEYSTORE_PATH, "/shared/keystore.jks")
      .set(FRONTEND_SSL_KEYSTORE_PASSWORD, "shared-password")
    assert(conf.get(FRONTEND_REST_SSL_KEYSTORE_PATH) === Some("/shared/keystore.jks"))
    assert(KyuubiRestFrontendService.keyStorePassword(conf) === Some("shared-password"))

    conf.set(FRONTEND_REST_SSL_KEYSTORE_PATH, "/rest/keystore.jks")
      .set(FRONTEND_REST_SSL_KEYSTORE_PASSWORD, "rest-password")
    assert(conf.get(FRONTEND_REST_SSL_KEYSTORE_PATH) === Some("/rest/keystore.jks"))
    assert(KyuubiRestFrontendService.keyStorePassword(conf) === Some("rest-password"))
  }

  test("SSL: keystore password of the RESTful frontend takes precedence over the shared one") {
    val providerPath = s"jceks://file${Utils.createTempDir().toAbsolutePath}/rest-ssl.jceks"
    val providerConf = new Configuration(false)
    providerConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    val provider = CredentialProviderFactory.getProviders(providerConf).get(0)
    provider.createCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key, "shared-alias".toCharArray)
    provider.flush()

    val conf = KyuubiConf(loadSysDefault = false)
      .set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
      .set(KyuubiConf.HADOOP_CREDENTIAL_CACHE_TTL, 0L)
    assert(KyuubiRestFrontendService.keyStorePassword(conf) === Some("shared-alias"))

    // a literal value of the REST key wins over an alias of the shared key
    conf.set(FRONTEND_REST_SSL_KEYSTORE_PASSWORD, "rest-literal")
    assert(KyuubiRestFrontendService.keyStorePassword(conf) === Some("rest-literal"))

    // an alias of the REST key wins over its literal value
    provider.createCredentialEntry(
      FRONTEND_REST_SSL_KEYSTORE_PASSWORD.key,
      "rest-alias".toCharArray)
    provider.flush()
    assert(KyuubiRestFrontendService.keyStorePassword(conf) === Some("rest-alias"))
  }
}
