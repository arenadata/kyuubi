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

import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLSocket

import io.grpc.{ManagedChannel, Status, StatusRuntimeException, TlsChannelCredentials}
import io.grpc.netty.NettyChannelBuilder
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{Utils, WithSparkConnectServer}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_TRANSPORT_SSL
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.server.grpc.KyuubiEngineTokenClientInterceptor
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor
import org.apache.kyuubi.session.KyuubiSessionImpl
import org.apache.kyuubi.shaded.spark.connect.proto.{AnalyzePlanRequest, SparkConnectServiceGrpc, UserContext}

/**
 * Spark Connect through a Kyuubi server whose Thrift and gRPC connections to the engine use TLS
 * and the engine token. The engine resolves its key store password through the credential
 * provider path the server forwards. Runs the shaded engine jar, so relocation mistakes in the
 * engine TLS code show up here. Canceled unless the Spark profile of the build serves Spark
 * Connect with TLS (spark.connect.tls.support=native), as such an engine does not start otherwise.
 * The engines run on the Spark of SPARK_HOME, or else the one downloaded for the Spark profile, so
 * that Spark must have Spark Connect TLS as well: otherwise the engines fail to start, naming
 * kyuubi.engine.ssl.enabled.
 */
class KyuubiSparkConnectEngineSslSuite extends WithSparkConnectServer {
  import KyuubiOperationWithEngineSslSuite._

  private val secret = s"engine-ssl-secret-${UUID.randomUUID()}"

  private lazy val credentialProviderPath = createCredentialProvider(
    materialDir,
    ENGINE_SSL_KEYSTORE_PASSWORD.key -> passwordOf(goodKeyStore))

  override protected lazy val conf: KyuubiConf = {
    val settings = engineSslServerConf(goodKeyStore) ++ Map(
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      // engines register, and are verified against, their IP address, as on Kubernetes
      FRONTEND_CONNECTION_URL_USE_HOSTNAME.key -> "false",
      ENGINE_SECURITY_ENABLED.key -> "true",
      ENGINE_SECURITY_SECRET_PROVIDER.key -> "simple",
      SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET.key -> secret,
      CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH -> credentialProviderPath,
      // a second server joins in one test, and two servers cannot share the metrics port
      "kyuubi.metrics.enabled" -> "false")
    val kyuubiConf = KyuubiConf()
    settings.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  private var spark: SparkSession = _

  // A build fact, set by the scalatest-maven-plugin from the Maven property of the Spark profile.
  // The engine module checks that its compiled Spark Connect TLS support matches it.
  private val sparkConnectTlsSupported: Boolean =
    sys.props.get("spark.connect.tls.support").contains("native")

  private def assumeSparkConnectTls(): Unit = assume(
    sparkConnectTlsSupported,
    "the Spark profile of this build cannot serve Spark Connect with TLS " +
      "(spark.connect.tls.support is not native)")

  override def beforeAll(): Unit = {
    super.beforeAll()
    // KyuubiFunSuite.beforeAll resets it after the server initialized it
    InternalSecurityAccessor.initialize(conf, true)
    if (sparkConnectTlsSupported) {
      spark = SparkSession.builder().remote(s"sc://$connectUrl").getOrCreate()
    }
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      try super.afterAll()
      finally InternalSecurityAccessor.reset()
    }
  }

  /** The Kyuubi session behind the Spark Connect session of this suite. */
  private def connectSession(): KyuubiSessionImpl = {
    val sessions = sessionsOf(server).filter(_.engineConnectUrl.isDefined)
    assert(sessions.size === 1)
    sessions.head
  }

  private def selectOne(session: SparkSession): Int =
    session.sql("SELECT 1").collect().head.getInt(0)

  private def tlsChannel(target: String, withToken: Boolean): ManagedChannel = {
    val credentials = TlsChannelCredentials.newBuilder()
      .trustManager(trustManagers(trustStore): _*)
      .build()
    val builder = NettyChannelBuilder.forTarget(target, credentials)
    if (withToken) builder.intercept(new KyuubiEngineTokenClientInterceptor)
    builder.build()
  }

  private def withChannel[T](channel: ManagedChannel)(f: ManagedChannel => T): T = {
    try f(channel)
    finally {
      channel.shutdownNow()
      channel.awaitTermination(10, TimeUnit.SECONDS)
    }
  }

  /** Calls AnalyzePlan(SparkVersion) on the engine, bounded by a deadline. */
  private def sparkVersion(channel: ManagedChannel): String = {
    val request = AnalyzePlanRequest.newBuilder()
      .setSessionId(UUID.randomUUID().toString)
      .setUserContext(UserContext.newBuilder().setUserId(Utils.currentUser))
      .setSparkVersion(AnalyzePlanRequest.SparkVersion.newBuilder())
      .build()
    SparkConnectServiceGrpc.newBlockingStub(channel)
      .withDeadlineAfter(TIMEOUT_MS, TimeUnit.MILLISECONDS)
      .analyzePlan(request)
      .getSparkVersion
      .getVersion
  }

  test("Spark Connect SQL runs through TLS connections to the engine") {
    assumeSparkConnectTls()
    assert(selectOne(spark) === 1)
    // several Arrow batches streamed from the engine through the Kyuubi server
    val rows = 50000
    val result = spark.sql(s"SELECT id, repeat('x', 100) AS payload FROM range(0, $rows)")
      .collect()
    assert(result.length === rows)
    assert(result.map(_.getLong(0)).sum === rows.toLong * (rows - 1) / 2)
    assert(result.forall(_.getString(1).length == 100))

    val session = connectSession()
    assert(session.engineSslEnabled)
    val sockets = engineSockets(session.client)
    assert(sockets.nonEmpty)
    assert(sockets.forall(_.isInstanceOf[SSLSocket]))
    val node = session.getEngineNode.getOrElse(fail("the engine is not registered"))
    assert(node.attributes.get(KYUUBI_ENGINE_TRANSPORT_SSL) === Some("true"))
    // one certificate covers both listeners, as both advertise the same host
    assert(session.engineConnectUrl.map(hostOf) === Some(node.host))
  }

  test("the engine serves Spark Connect only to TLS clients with an engine token") {
    assumeSparkConnectTls()
    assert(selectOne(spark) === 1)
    val target = connectSession().engineConnectUrl.get

    withChannel(tlsChannel(target, withToken = true)) { channel =>
      assert(sparkVersion(channel) === spark.version)
    }
    withChannel(tlsChannel(target, withToken = false)) { channel =>
      val e = intercept[StatusRuntimeException](sparkVersion(channel))
      assert(e.getStatus.getCode === Status.Code.UNAUTHENTICATED)
    }
    withChannel(NettyChannelBuilder.forTarget(target).usePlaintext().build()) { channel =>
      val e = intercept[StatusRuntimeException](sparkVersion(channel))
      // refused by the TLS listener, not left waiting until the deadline
      assert(e.getStatus.getCode !== Status.Code.DEADLINE_EXCEEDED)
    }
  }

  test("a Kyuubi server that does not trust the engine certificate fails the session") {
    assumeSparkConnectTls()
    assert(selectOne(spark) === 1)
    val engineNode = connectSession().getEngineNode.getOrElse(fail("the engine is not registered"))

    val foreignTrustStore = generateTrustStore(materialDir, foreignKeyStore)
    val untrustingConf = conf.clone.set(SERVER_NAME, "kyuubi-engine-ssl-untrusting")
    serverSslConf(foreignTrustStore).foreach { case (k, v) => untrustingConf.set(k, v) }
    val untrusting = KyuubiServer.startServer(untrustingConf)
    try {
      val appender = new LogAppender("opening a session on an untrusted engine")
      withLogAppender(appender, Seq(classOf[KyuubiSessionImpl].getName)) {
        intercept[Exception] {
          val url = s"sc://${untrusting.frontendServices.head.connectionUrl}"
          val other = SparkSession.builder().remote(url).getOrCreate()
          try selectOne(other)
          finally other.stop()
        }
      }
      val reasons = appender.loggingEvents.flatMap(messagesOf)
      assert(
        reasons.exists(m => m.contains("is not trusted by") && m.contains(s"$foreignTrustStore")),
        reasons.mkString("\n"))
      eventually(timeout(30.seconds), interval(500.millis)) {
        assert(sessionsOf(untrusting).isEmpty)
      }
    } finally {
      untrusting.stop()
    }

    // an untrusted certificate is no TLS mismatch: the engine stays and keeps serving
    assert(connectSession().getEngineNode.map(_.nodeName) === Some(engineNode.nodeName))
    assert(spark.sql("SELECT 2").collect().head.getInt(0) === 2)
  }
}
