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

package org.apache.kyuubi.engine

import java.util.Locale

import scala.util.Try

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_TRANSPORT_SSL

/**
 * Decides, from the Kyuubi server configuration alone, whether the internal connections to an
 * engine use TLS and which `kyuubi.engine.ssl.*` settings reach the engine.
 */
object EngineSslPolicy {

  val SUPPORTED_ENGINE_TYPES: Set[EngineType.EngineType] = Set(EngineType.SPARK_SQL)

  val PREFIXES: Seq[String] = Seq("kyuubi.engine.ssl.", "spark.kyuubi.engine.ssl.")

  // What an engine needs to serve TLS. Passwords never go on the engine command line, and the
  // trust store and the expected host name only matter to the Kyuubi server.
  val FORWARDED_KEYS: Seq[String] = Seq(
    ENGINE_SSL_KEYSTORE_PATH.key,
    ENGINE_SSL_KEYSTORE_TYPE.key,
    ENGINE_SSL_KEYSTORE_ALGORITHM.key,
    ENGINE_SSL_DISALLOWED_PROTOCOLS.key,
    ENGINE_SSL_INCLUDE_CIPHER_SUITES.key)

  def isEngineSslKey(key: String): Boolean = PREFIXES.exists(key.startsWith)

  def supports(engineType: String): Boolean =
    Try(EngineType.withName(engineType.trim.toUpperCase(Locale.ROOT))).toOption
      .exists(SUPPORTED_ENGINE_TYPES.contains)

  /**
   * Remove every engine TLS key from the session conf, wherever it came from (server
   * defaults, an engine profile or a conf advisor), then set the server's decision and the
   * explicitly configured server values the engine needs. Engines that support engine TLS get
   * the decision both ways, so that a `spark.kyuubi.engine.ssl.enabled` in the engine-side
   * spark-defaults.conf cannot override it.
   *
   * @return whether the engine uses TLS, and the names of the removed keys
   */
  def applyTo(serverConf: KyuubiConf, sessionConf: KyuubiConf): (Boolean, Seq[String]) = {
    val stripped = sessionConf.getAll.keys.filter(isEngineSslKey).toSeq.sorted
    stripped.foreach(key => sessionConf.unset(key))
    val supported = Try(sessionConf.get(ENGINE_TYPE)).toOption.exists(supports)
    val enabled = supported && serverConf.get(ENGINE_SSL_ENABLED)
    if (supported) {
      sessionConf.set(ENGINE_SSL_ENABLED, enabled)
    }
    if (enabled) {
      FORWARDED_KEYS.foreach { key =>
        serverConf.getOption(key).foreach(value => sessionConf.set(key, value))
      }
    }
    (enabled, stripped)
  }

  def withoutEngineSslKeys(conf: Map[String, String]): Map[String, String] =
    conf.filterNot { case (key, _) => isEngineSslKey(key) }

  /**
   * Compare the TLS state an engine advertises in service discovery with what this Kyuubi
   * server expects. An engine without the attribute is a plaintext engine.
   *
   * @param attrs the discovery attributes of the engine, None if its node is gone
   * @return a message explaining the mismatch, None if there is none or it cannot be told
   */
  def diagnoseMismatch(
      expectedTls: Boolean,
      attrs: Option[Map[String, String]],
      hostPort: String): Option[String] = attrs.flatMap { attributes =>
    val actualTls = attributes.get(KYUUBI_ENGINE_TRANSPORT_SSL).contains("true")
    if (actualTls != expectedTls) {
      Some(s"Engine $hostPort was started with TLS ${if (actualTls) "on" else "off"} but " +
        s"this Kyuubi server has ${ENGINE_SSL_ENABLED.key}=$expectedTls; deregistering it so " +
        "a new engine is launched. Make sure every Kyuubi server in the namespace uses the " +
        "same value and the engine jar supports engine TLS.")
    } else {
      None
    }
  }

  /** Warnings about the Kyuubi server configuration, naming keys and never values. */
  def startupWarnings(serverConf: KyuubiConf): Seq[String] = {
    val literalPasswords =
      Seq(ENGINE_SSL_KEYSTORE_PASSWORD.key, s"spark.${ENGINE_SSL_KEYSTORE_PASSWORD.key}")
        .filter(key => serverConf.getOption(key).isDefined)
        .map { key =>
          s"$key is set in the Kyuubi server configuration, but it is never forwarded to " +
            "engines. Provide the engine key store password as a Hadoop credential provider " +
            s"alias named ${ENGINE_SSL_KEYSTORE_PASSWORD.key}, or in the engine-side " +
            "spark-defaults.conf."
        }
    val tlsEnabled = serverConf.get(ENGINE_SSL_ENABLED)
    val defaultTrust =
      if (tlsEnabled && serverConf.get(ENGINE_SSL_TRUSTSTORE_PATH).isEmpty) {
        Seq(s"${ENGINE_SSL_ENABLED.key} is true but ${ENGINE_SSL_TRUSTSTORE_PATH.key} is not " +
          "set: engine certificates are verified against the JVM default trust store, so any " +
          "host registered in service discovery that holds a publicly issued certificate for " +
          "its own name is accepted. Set it to a trust store holding only the cluster CA.")
      } else {
        Nil
      }
    val noEngineAuth =
      if (tlsEnabled && !serverConf.get(ENGINE_SECURITY_ENABLED)) {
        Seq(s"${ENGINE_SSL_ENABLED.key} is true but ${ENGINE_SECURITY_ENABLED.key} is false, " +
          "engines accept any TLS client.")
      } else {
        Nil
      }
    literalPasswords ++ defaultTrust ++ noEngineAuth
  }
}
