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

import java.io.IOException
import java.util.UUID
import javax.net.ssl.SSLSocket

import scala.concurrent.duration._
import scala.reflect.ClassTag

import org.sparkproject.connect.grpc.Metadata

import org.apache.kyuubi.{BoundedCall, KyuubiFunSuite}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_CONNECT_URL, KYUUBI_ENGINE_TRANSPORT_SSL}
import org.apache.kyuubi.engine.spark.connect.SparkConnectTls
import org.apache.kyuubi.service.TBinaryFrontendService
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TCloseSessionReq
import org.apache.kyuubi.shaded.thrift.transport.TTransportException
import org.apache.kyuubi.util.EngineSslUtils

class SparkEngineSslMisconfigSuite extends KyuubiFunSuite {
  import SparkEngineSslSuite._

  private val passwordKey = ENGINE_SSL_KEYSTORE_PASSWORD.key

  override def afterAll(): Unit = {
    // the createSpark failures leave engine TLS on in the configuration shared by the JVM
    try SparkSQLEngine.setupConf()
    finally super.afterAll()
  }

  // an in-JVM engine with its own settings, started and stopped by each test
  private class Engine(settings: Map[String, String]) extends WithSparkSQLEngine {
    // the test certificates cover localhost, also when the suite runs outside Maven
    override def withKyuubiConf: Map[String, String] =
      settings + (FRONTEND_THRIFT_BINARY_BIND_HOST.key -> "localhost")
    def get: SparkSQLEngine = engine
  }

  private def withEngine(settings: Map[String, String])(f: SparkSQLEngine => Unit): Unit = {
    val harness = new Engine(settings)
    try {
      harness.startSparkEngine()
      f(harness.get)
    } finally {
      harness.stopSparkEngine()
    }
  }

  // setupConf and createSpark as SparkSQLEngine.main runs them, expecting createSpark to fail
  private def createSparkFailure[E <: Exception: ClassTag](
      settings: Map[String, String])(afterSetup: => Unit = ()): E = {
    var failure: Option[E] = None
    withSystemProperty(settings) {
      SparkSQLEngine.setupConf()
      afterSetup
      failure = Some(intercept[E](SparkSQLEngine.createSpark()))
    }
    assert(SparkSQLEngine.engineSslMaterial.isEmpty)
    failure.get
  }

  test("a missing key store path stops the engine before Spark starts, naming the key") {
    val e = createSparkFailure[IllegalArgumentException](Map(ENGINE_SSL_ENABLED.key -> "true"))()
    assert(e.getMessage.contains(ENGINE_SSL_KEYSTORE_PATH.key))
    assert(e.getMessage.contains(ENGINE_SSL_ENABLED.key))
  }

  test("an unresolvable key store password names the alias and the engine side setting") {
    val e = createSparkFailure[IllegalArgumentException](Map(
      ENGINE_SSL_ENABLED.key -> "true",
      ENGINE_SSL_KEYSTORE_PATH.key -> goodKeyStore.toString,
      ENGINE_SSL_KEYSTORE_TYPE.key -> storeTypeOf(goodKeyStore)))()
    assert(e.getMessage.contains(s"$passwordKey not configured"))
    assert(e.getMessage.contains("credential provider alias"))
    assert(e.getMessage.contains(s"spark.$passwordKey"))
  }

  test("a wrong key store password names the key and never the value") {
    val wrong = s"wrong-${UUID.randomUUID()}"
    val e = createSparkFailure[IllegalArgumentException](Map(
      ENGINE_SSL_ENABLED.key -> "true",
      ENGINE_SSL_KEYSTORE_PATH.key -> goodKeyStore.toString,
      ENGINE_SSL_KEYSTORE_TYPE.key -> storeTypeOf(goodKeyStore),
      s"spark.$passwordKey" -> wrong)) {
      // taken out of the configuration and the system properties before the key store is read
      assert(SparkSQLEngine.kyuubiConf.getOption(passwordKey).isEmpty)
      assert(!SparkSQLEngine.kyuubiConf.getAll.values.exists(_.contains(wrong)))
      assert(System.getProperty(s"spark.$passwordKey") === null)
      assert(System.getProperty(passwordKey) === null)
    }
    assert(e.getMessage.contains(passwordKey))
    assert(e.getMessage.contains(goodKeyStore.toString))
    assert(!causes(e).exists(t => String.valueOf(t.getMessage).contains(wrong)))
  }

  test("listeners never fall back to plaintext when the key material was not loaded") {
    val previous = SparkSQLEngine.engineSslMaterial
    SparkSQLEngine.engineSslMaterial = None
    try {
      val enabled = KyuubiConf(false).set(ENGINE_SSL_ENABLED, true)
      val e = intercept[IllegalStateException](SparkSQLEngine.engineSslMaterialFor(enabled))
      assert(e.getMessage.contains(ENGINE_SSL_ENABLED.key))
      assert(SparkSQLEngine.engineSslMaterialFor(KyuubiConf(false)).isEmpty)
    } finally {
      SparkSQLEngine.engineSslMaterial = previous
    }
  }

  test("with TLS and without Spark Connect, only the Thrift frontend serves TLS") {
    val settings = engineSslConf() ++ Map(FRONTEND_PROTOCOLS.key -> "THRIFT_BINARY")
    withEngine(settings) { engine =>
      val frontend = engine.frontendServices.head.asInstanceOf[TBinaryFrontendService]
      val url = frontend.connectionUrl
      assert(frontend.engineSslEnabled)
      assert(frontend.attributes.get(KYUUBI_ENGINE_TRANSPORT_SSL) === Some("true"))
      assert(engine.connectUrl.isEmpty)

      withTlsThriftClient(url, "anonymous") { client =>
        val resp = openSession(client, "anonymous")
        assert(!resp.getConfiguration.containsKey(KYUUBI_ENGINE_CONNECT_URL))
        client.CloseSession(new TCloseSessionReq(resp.getSessionHandle))
      }
      BoundedCall.within(2.minutes) {
        intercept[TTransportException] {
          withPlainThriftClient(url, "anonymous")(openSession(_, "anonymous"))
        }
      }
    }
  }

  test("without Spark Connect TLS support, an engine with Spark Connect and TLS fails to start") {
    assume(!SparkConnectTls.supported, "this engine build serves Spark Connect with TLS")
    val settings = engineSslConf() ++ Map(FRONTEND_PROTOCOLS.key -> "THRIFT_BINARY,SPARK_CONNECT")
    // before the SparkContext exists, and before the key store is loaded
    val e = createSparkFailure[IllegalStateException](settings)()
    assert(e.getMessage.contains(ENGINE_SSL_ENABLED.key))
    assert(e.getMessage.contains("cannot serve Spark Connect with TLS"))

    // Spark Connect itself never starts in plaintext when TLS is asked for
    withEngine(Map(FRONTEND_PROTOCOLS.key -> "THRIFT_BINARY")) { engine =>
      val conf = KyuubiConf(false)
      engineSslConf().foreach { case (k, v) => conf.set(k, v) }
      val material = EngineSslUtils.loadServerMaterial(conf)
      val refused = intercept[IllegalStateException](SparkConnectTls.start(engine.spark, material))
      assert(refused.getMessage === e.getMessage)
    }
  }

  test("Spark Connect serves TLS with the protocols and cipher suites of the Thrift listener") {
    assume(SparkConnectTls.supported, SPARK_CONNECT_TLS_UNSUPPORTED)
    val cipherSuite = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
    val settings = engineSslConf() ++ Map(
      FRONTEND_PROTOCOLS.key -> "THRIFT_BINARY,SPARK_CONNECT",
      ENGINE_SSL_DISALLOWED_PROTOCOLS.key -> "SSLv2,SSLv3,TLSv1,TLSv1.1,TLSv1.3",
      ENGINE_SSL_INCLUDE_CIPHER_SUITES.key -> cipherSuite)
    withEngine(settings) { engine =>
      Seq(engine.frontendServices.head.connectionUrl, engine.connectUrl.get).foreach { url =>
        handshake(url, "TLSv1.2", Some(cipherSuite))
        intercept[IOException](handshake(url, "TLSv1.3", None))
        intercept[IOException] {
          handshake(url, "TLSv1.2", Some("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"))
        }
      }
    }
  }

  test("cipher suites that gRPC clients never offer stop an engine with Spark Connect early") {
    assume(SparkConnectTls.supported, SPARK_CONNECT_TLS_UNSUPPORTED)
    val cbcOnly = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"
    val settings = engineSslConf() ++ Map(
      FRONTEND_PROTOCOLS.key -> "THRIFT_BINARY,SPARK_CONNECT",
      ENGINE_SSL_INCLUDE_CIPHER_SUITES.key -> cbcOnly)
    // before the SparkContext exists, as every Spark Connect handshake would fail
    val e = createSparkFailure[IllegalArgumentException](settings)()
    assert(e.getMessage.contains(ENGINE_SSL_INCLUDE_CIPHER_SUITES.key))
    assert(e.getMessage.contains(cbcOnly))
    assert(e.getMessage.contains("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"))
  }

  test("an engine-side spark.ssl.connect.enabled cannot turn Spark Connect TLS on") {
    val settings = Map(
      FRONTEND_PROTOCOLS.key -> "THRIFT_BINARY,SPARK_CONNECT",
      s"$SSL_CONNECT_PREFIX.enabled" -> "true",
      s"$SSL_CONNECT_PREFIX.keyStore" -> goodKeyStore.toString,
      s"$SSL_CONNECT_PREFIX.keyStorePassword" -> passwordOf(goodKeyStore))
    withEngine(settings) { engine =>
      withChannel(plainChannel(engine.connectUrl.get)) { channel =>
        assert(sparkVersion(channel, new Metadata()) === engine.spark.version)
      }
    }
  }

  // a TLS handshake with only the given protocol and cipher suite enabled on the client
  private def handshake(url: String, protocol: String, cipherSuite: Option[String]): Unit = {
    val socket = clientContext.socketFactory.createSocket(hostOf(url), portOf(url))
      .asInstanceOf[SSLSocket]
    try {
      socket.setSoTimeout(30000)
      socket.setEnabledProtocols(Array(protocol))
      cipherSuite.foreach(suite => socket.setEnabledCipherSuites(Array(suite)))
      socket.startHandshake()
    } finally {
      socket.close()
    }
  }
}
