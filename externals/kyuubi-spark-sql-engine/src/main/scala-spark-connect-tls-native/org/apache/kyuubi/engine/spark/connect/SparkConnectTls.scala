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

package org.apache.kyuubi.engine.spark.connect

import scala.collection.JavaConverters._

import io.netty.handler.codec.http2.Http2SecurityUtil
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service.SparkConnectService

import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SSL_DISALLOWED_PROTOCOLS, ENGINE_SSL_ENABLED, ENGINE_SSL_INCLUDE_CIPHER_SUITES}
import org.apache.kyuubi.util.EngineSslServerMaterial

/**
 * Spark Connect TLS of the engine, for Spark builds with Spark Connect TLS
 * (`spark.connect.tls.support=native`). Builds for other Spark versions compile the variant in
 * `src/main/scala-spark-connect-tls-none` instead.
 */
object SparkConnectTls {

  // gRPC clients, the Kyuubi server among them, offer only the cipher suites of HTTP/2 over TLS
  private val grpcCipherSuites: Seq[String] = Http2SecurityUtil.CIPHERS.asScala.toList

  /** Whether this engine build can serve Spark Connect with TLS. */
  val supported: Boolean = true

  /** Fails unless this engine build can serve Spark Connect with TLS. */
  def requireSupported(): Unit = {}

  /**
   * Fails unless the cipher suites of the engine TLS policy include one that gRPC clients offer,
   * as every Spark Connect handshake would fail otherwise.
   */
  def checkPolicy(material: EngineSslServerMaterial): Unit = {
    if (material.cipherSuites.nonEmpty &&
      !material.cipherSuites.exists(grpcCipherSuites.contains)) {
      throw new IllegalArgumentException(
        s"None of the cipher suites that ${ENGINE_SSL_INCLUDE_CIPHER_SUITES.key} enables " +
          s"(${material.cipherSuites.mkString(",")}) can serve Spark Connect: gRPC clients, the " +
          s"Kyuubi server among them, only offer ${grpcCipherSuites.mkString(",")}")
    }
  }

  /**
   * Starts the Spark Connect gRPC server, serving only TLS with the key material and the
   * protocol and cipher suite policy of the engine Thrift listener. Spark ignores
   * `spark.ssl.connect.*` here. Returns the bound port.
   */
  def start(spark: SparkSession, material: EngineSslServerMaterial): Int = {
    checkPolicy(material)
    try {
      SparkConnectService.startWithTls(
        spark.sparkContext,
        material.keyManagerFactory,
        material.protocols,
        material.cipherSuites)
    } catch {
      // the engine runs on another Spark than the one it was built for
      case e: NoSuchMethodError =>
        throw new IllegalStateException(
          s"${ENGINE_SSL_ENABLED.key} is true, but the Spark of this engine ($SPARK_VERSION) " +
            "cannot serve Spark Connect with TLS: it has no SparkConnectService.startWithTls. " +
            "Spark Connect TLS needs an ADH Spark build with Spark Connect TLS.",
          e)
      // the TLS provider of Spark Connect may support less than the JVM does
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(
          "Spark Connect cannot serve TLS with the engine policy of " +
            s"${ENGINE_SSL_DISALLOWED_PROTOCOLS.key} and " +
            s"${ENGINE_SSL_INCLUDE_CIPHER_SUITES.key}: ${e.getMessage}",
          e)
    }
  }
}
