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

package org.apache.kyuubi.engine.spark

import java.nio.file.Path
import java.sql.{DriverManager, SQLException}
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLException

import scala.concurrent.duration._

import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.SparkEnv
import org.apache.spark.connect.proto.{AnalyzePlanRequest, SparkConnectServiceGrpc, UserContext}
import org.sparkproject.connect.grpc.{ManagedChannel, Metadata, Status, StatusRuntimeException, TlsChannelCredentials}
import org.sparkproject.connect.grpc.netty.NettyChannelBuilder
import org.sparkproject.connect.grpc.stub.MetadataUtils

import org.apache.kyuubi.{BoundedCall, KeyStoreTestUtils, Utils}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_CONNECT_URL, KYUUBI_ENGINE_TRANSPORT_SSL}
import org.apache.kyuubi.engine.spark.connect.{KyuubiEngineTokenServerInterceptor, SparkConnectTls}
import org.apache.kyuubi.engine.spark.events.EngineEvent
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.service.TBinaryFrontendService
import org.apache.kyuubi.service.authentication.{InternalSecurityAccessor, PlainSASLHelper}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TCLIService, TCloseSessionReq, TOpenSessionReq, TOpenSessionResp, TStatusCode}
import org.apache.kyuubi.shaded.thrift.protocol.TBinaryProtocol
import org.apache.kyuubi.shaded.thrift.transport.{TSocket, TTransportException}
import org.apache.kyuubi.util.{EngineSslClientContext, EngineSslUtils}

/**
 * A Spark engine with engine TLS, engine security and Spark Connect, configured the way a Kyuubi
 * server launches it: the key store password only resolves through the credential provider
 * path that the server forwards as a Spark Hadoop setting. Engines built without Spark Connect
 * TLS cannot start such an engine (see SparkEngineSslMisconfigSuite), so the engine only serves
 * Thrift there and the Spark Connect tests are canceled.
 */
class SparkEngineSslSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {
  import SparkEngineSslSuite._

  private val secret = s"engine-ssl-secret-${UUID.randomUUID()}"

  // a wrong literal, as from an engine side spark-defaults.conf: the alias takes precedence,
  // and the literal must still be scrubbed
  private val decoyPassword = s"decoy-${UUID.randomUUID()}"

  private lazy val credentialProviderPath = createCredentialProvider(
    materialDir,
    ENGINE_SSL_KEYSTORE_PASSWORD.key -> passwordOf(goodKeyStore))

  private val protocols =
    if (SparkConnectTls.supported) "THRIFT_BINARY,SPARK_CONNECT" else "THRIFT_BINARY"

  override def withKyuubiConf: Map[String, String] = Map(
    FRONTEND_PROTOCOLS.key -> protocols,
    // the test certificates cover localhost, also when the suite runs outside Maven
    FRONTEND_THRIFT_BINARY_BIND_HOST.key -> "localhost",
    ENGINE_SSL_ENABLED.key -> "true",
    ENGINE_SSL_KEYSTORE_PATH.key -> goodKeyStore.toString,
    ENGINE_SSL_KEYSTORE_TYPE.key -> storeTypeOf(goodKeyStore),
    s"spark.${ENGINE_SSL_KEYSTORE_PASSWORD.key}" -> decoyPassword,
    s"spark.hadoop.${CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH}" -> credentialProviderPath,
    ENGINE_SECURITY_ENABLED.key -> "true",
    ENGINE_SECURITY_SECRET_PROVIDER.key -> "simple",
    SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET.key -> secret,
    // SparkProcessBuilder sets it for engines launched by a Kyuubi server with engine security
    INTERCEPTOR_CLASSES_KEY -> classOf[KyuubiEngineTokenServerInterceptor].getName)

  override protected def jdbcUrl: String =
    s"jdbc:hive2://$connectionUrl/;ssl=true;sslTrustStore=$jksTrustStore;" +
      s"trustStorePassword=${passwordOf(jksTrustStore)};$JDBC_TIMEOUTS"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // KyuubiFunSuite.beforeAll resets it after WithSparkSQLEngine started the engine
    InternalSecurityAccessor.initialize(kyuubiConf, isServer = false)
  }

  override def afterAll(): Unit = {
    try super.afterAll()
    finally InternalSecurityAccessor.reset()
  }

  private def token: String = InternalSecurityAccessor.get().issueToken()

  private def tokenHeader(value: String): Metadata =
    headers(KyuubiEngineTokenServerInterceptor.TOKEN_HEADER -> value)

  private def connectUrl: String = engine.connectUrl.get

  private def assumeSparkConnectTls(): Unit =
    assume(SparkConnectTls.supported, SPARK_CONNECT_TLS_UNSUPPORTED)

  test("the Thrift frontend serves TLS and advertises it in service discovery") {
    val frontend = engine.frontendServices.head.asInstanceOf[TBinaryFrontendService]
    assert(frontend.engineSslEnabled)
    assert(frontend.attributes.get(KYUUBI_ENGINE_TRANSPORT_SSL) === Some("true"))
  }

  test("JDBC over TLS runs a query, a plaintext JDBC connection fails") {
    val connection = DriverManager.getConnection(jdbcUrl, user, token)
    try {
      val rs = connection.createStatement().executeQuery("SELECT 1")
      assert(rs.next())
      assert(rs.getInt(1) === 1)
    } finally {
      connection.close()
    }

    BoundedCall.within(2.minutes) {
      intercept[SQLException] {
        DriverManager.getConnection(s"jdbc:hive2://$connectionUrl/;$JDBC_TIMEOUTS", user, token)
          .close()
      }
    }
  }

  test("a plaintext Thrift client is rejected, a TLS client opens a session") {
    BoundedCall.within(2.minutes) {
      intercept[TTransportException] {
        withPlainThriftClient(connectionUrl, token)(openSession(_, token))
      }
    }
    withTlsThriftClient(connectionUrl, token) { client =>
      val resp = openSession(client, token)
      assert(Option(resp.getConfiguration.get(KYUUBI_ENGINE_CONNECT_URL)) === engine.connectUrl)
      client.CloseSession(new TCloseSessionReq(resp.getSessionHandle))
    }
  }

  test("the Spark Connect URL uses the Thrift host and the Spark Connect port") {
    assumeSparkConnectTls()
    assert(connectUrl === s"${hostOf(connectionUrl)}:${SparkEnv.get.conf.get(BINDING_PORT_KEY)}")
  }

  test("Spark Connect serves TLS and keeps the engine token interceptor") {
    assumeSparkConnectTls()
    withChannel(tlsChannel(connectUrl)) { channel =>
      assert(sparkVersion(channel, tokenHeader(token)) === spark.version)

      val missingToken = intercept[StatusRuntimeException](sparkVersion(channel, new Metadata()))
      assert(missingToken.getStatus.getCode === Status.Code.UNAUTHENTICATED)
      assert(missingToken.getStatus.getDescription.contains("Missing Kyuubi engine token"))

      val forgedToken =
        intercept[StatusRuntimeException](sparkVersion(channel, tokenHeader("forged")))
      assert(forgedToken.getStatus.getCode === Status.Code.UNAUTHENTICATED)
    }
  }

  test("Spark Connect rejects plaintext and untrusting clients") {
    assumeSparkConnectTls()
    withChannel(plainChannel(connectUrl)) { channel =>
      val e = intercept[StatusRuntimeException](sparkVersion(channel, tokenHeader(token)))
      // the status code depends on which side closes the connection first, but the call must
      // fail as soon as the TLS listener drops the cleartext preface, not run into its deadline
      assert(e.getStatus.getCode !== Status.Code.DEADLINE_EXCEEDED)
    }

    val foreignTrustStore = generateTrustStore(materialDir, foreignKeyStore)
    withChannel(tlsChannel(connectUrl, foreignTrustStore)) { channel =>
      val e = intercept[StatusRuntimeException](sparkVersion(channel, tokenHeader(token)))
      assert(e.getStatus.getCode === Status.Code.UNAVAILABLE)
      assert(causes(e).exists(_.isInstanceOf[SSLException]))
    }
  }

  test("the key store password does not stay in the engine configuration") {
    val key = ENGINE_SSL_KEYSTORE_PASSWORD.key
    val password = passwordOf(goodKeyStore)
    assert(kyuubiConf.getOption(key).isEmpty)
    assert(System.getProperty(key) === null)
    assert(System.getProperty(s"spark.$key") === null)
    assert(spark.sparkContext.getConf.getOption(s"spark.$key").forall(_.isEmpty))
    assert(!kyuubiConf.getAll.values.exists(_.contains(decoyPassword)))
    Seq(SparkEnv.get.conf.getAll.toMap, spark.newSession().conf.getAll).foreach { conf =>
      assert(!conf.values.exists(value => value == password || value.contains(decoyPassword)))
    }

    val settings = EngineEvent(engine).settings
    assert(!settings.values.exists(_.contains(decoyPassword)))
    assert(!settings.values.exists(_ == password))
  }

  // keep it last, it stops the engine
  test("stopping the engine stops the Spark Connect TLS server") {
    assumeSparkConnectTls()
    val url = connectUrl
    engine.stop()
    withChannel(tlsChannel(url)) { channel =>
      val e = intercept[StatusRuntimeException](sparkVersion(channel, tokenHeader(token)))
      assert(e.getStatus.getCode === Status.Code.UNAVAILABLE)
    }
  }
}

object SparkEngineSslSuite {

  val BINDING_PORT_KEY = "spark.connect.grpc.binding.port"
  val INTERCEPTOR_CLASSES_KEY = "spark.connect.grpc.interceptor.classes"

  val SSL_CONNECT_PREFIX = "spark.ssl.connect"

  val SPARK_CONNECT_TLS_UNSUPPORTED =
    "this engine build cannot serve Spark Connect with TLS (spark.connect.tls.support=none)"

  private val TIMEOUT_MS = 30000
  private val MAX_MESSAGE_SIZE = 64 * 1024 * 1024

  val JDBC_TIMEOUTS = s"connectTimeout=$TIMEOUT_MS;socketTimeout=$TIMEOUT_MS"

  /** Trusts the test CA only, like a Kyuubi server with kyuubi.engine.ssl.truststore.path. */
  lazy val clientContext: EngineSslClientContext = {
    val conf = KyuubiConf(false)
    serverSslConf().foreach { case (k, v) => conf.set(k, v) }
    EngineSslUtils.loadClientContext(conf)
  }

  def hostOf(url: String): String = url.substring(0, url.lastIndexOf(':'))

  def portOf(url: String): Int = url.substring(url.lastIndexOf(':') + 1).toInt

  def causes(e: Throwable): Seq[Throwable] =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(16).toList

  def withTlsThriftClient[T](url: String, password: String)(f: TCLIService.Iface => T): T = {
    val socket = EngineSslUtils.openClientSocket(
      clientContext,
      hostOf(url),
      portOf(url),
      TIMEOUT_MS,
      TIMEOUT_MS,
      MAX_MESSAGE_SIZE)
    try {
      val transport = PlainSASLHelper.getPlainTransport(Utils.currentUser, password, socket)
      transport.open()
      f(new TCLIService.Client(new TBinaryProtocol(transport)))
    } finally {
      socket.close()
    }
  }

  def withPlainThriftClient[T](url: String, password: String)(f: TCLIService.Iface => T): T = {
    val socket = new TSocket(hostOf(url), portOf(url), TIMEOUT_MS)
    try {
      val transport = PlainSASLHelper.getPlainTransport(Utils.currentUser, password, socket)
      transport.open()
      f(new TCLIService.Client(new TBinaryProtocol(transport)))
    } finally {
      socket.close()
    }
  }

  def openSession(client: TCLIService.Iface, password: String): TOpenSessionResp = {
    val req = new TOpenSessionReq()
    req.setUsername(Utils.currentUser)
    req.setPassword(password)
    val resp = client.OpenSession(req)
    assert(resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS, resp.getStatus)
    resp
  }

  def tlsChannel(
      target: String,
      trustStore: Path = KeyStoreTestUtils.trustStore): ManagedChannel = {
    val credentials = TlsChannelCredentials.newBuilder()
      .trustManager(trustManagers(trustStore): _*)
      .build()
    NettyChannelBuilder.forTarget(target, credentials).build()
  }

  def plainChannel(target: String): ManagedChannel =
    NettyChannelBuilder.forTarget(target).usePlaintext().build()

  def withChannel[T](channel: ManagedChannel)(f: ManagedChannel => T): T = {
    try f(channel)
    finally {
      channel.shutdownNow()
      channel.awaitTermination(10, TimeUnit.SECONDS)
    }
  }

  def headers(entries: (Metadata.Key[String], String)*): Metadata = {
    val metadata = new Metadata()
    entries.foreach { case (key, value) => metadata.put(key, value) }
    metadata
  }

  /** Calls AnalyzePlan(SparkVersion), bounded by a deadline. */
  def sparkVersion(channel: ManagedChannel, metadata: Metadata): String = {
    val stub = SparkConnectServiceGrpc.newBlockingStub(channel)
      .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
      .withDeadlineAfter(TIMEOUT_MS, TimeUnit.MILLISECONDS)
    val request = AnalyzePlanRequest.newBuilder()
      .setSessionId(UUID.randomUUID().toString)
      .setUserContext(UserContext.newBuilder().setUserId(Utils.currentUser).build())
      .setSparkVersion(AnalyzePlanRequest.SparkVersion.newBuilder().build())
      .build()
    stub.analyzePlan(request).getSparkVersion.getVersion
  }
}
