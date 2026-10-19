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

package org.apache.kyuubi.session

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SHARE_LEVEL, ENGINE_SSL_ENABLED, ENGINE_SSL_KEYSTORE_PATH, ENGINE_SSL_TRUSTSTORE_PATH}

class SessionManagerSuite extends KyuubiFunSuite {

  private def newManager(): NoopSessionManager = {
    val m = new NoopSessionManager
    m.initialize(KyuubiConf(loadSysDefault = false))
    m
  }

  test("credential-store conf keys are always restricted on the interactive path") {
    val m = newManager()
    try {
      Seq(
        "hadoop.security.credential.provider.path",
        "hadoop.security.credential.vault.token",
        "hadoop.security.credstore.java-keystore-provider.password-file",
        "spark.hadoop.hadoop.security.credential.provider.path",
        "spark.hadoop.hadoop.security.credential.vault.token",
        "spark.hadoop.hadoop.security.credstore.java-keystore-provider.password-file")
        .foreach { key =>
          val e = intercept[KyuubiSQLException](m.validateKey(key, "x"))
          assert(e.getMessage.contains("restrict"))
        }
    } finally {
      m.stop()
    }
  }

  test("credential-store conf keys are always restricted on the batch path") {
    val m = newManager()
    try {
      Seq(
        "hadoop.security.credential.provider.path",
        "spark.hadoop.hadoop.security.credential.provider.path").foreach { key =>
        val e = intercept[KyuubiSQLException](m.validateBatchConf(Map(key -> "vault://x:8200/y")))
        assert(e.getMessage.contains("restrict"))
      }
    } finally {
      m.stop()
    }
  }

  test("engine TLS conf keys are always restricted on the interactive path") {
    val m = newManager()
    try {
      Seq(
        ENGINE_SSL_ENABLED.key,
        s"set:hiveconf:${ENGINE_SSL_KEYSTORE_PATH.key}",
        s"set:hivevar:${ENGINE_SSL_TRUSTSTORE_PATH.key}",
        s"set:system:${ENGINE_SSL_ENABLED.key}",
        s"spark.${ENGINE_SSL_ENABLED.key}",
        s"set:hiveconf:spark.${ENGINE_SSL_KEYSTORE_PATH.key}").foreach { key =>
        val e = intercept[KyuubiSQLException](m.validateKey(key, "false"))
        assert(e.getMessage.contains("restrict"), key)
      }
      assert(m.validateKey(ENGINE_SHARE_LEVEL.key, "USER") ===
        Some(ENGINE_SHARE_LEVEL.key -> "USER"))
      // the Spark SSL options are no engine TLS keys
      Seq("spark.ssl.ui.enabled", "spark.ssl.connect.enabled").foreach { key =>
        assert(m.validateKey(key, "true") === Some(key -> "true"))
      }
    } finally {
      m.stop()
    }
  }

  test("engine TLS conf keys are always restricted on the batch path") {
    val m = newManager()
    try {
      Seq(
        ENGINE_SSL_ENABLED.key,
        s"spark.${ENGINE_SSL_KEYSTORE_PATH.key}").foreach { key =>
        val e = intercept[KyuubiSQLException](m.validateBatchConf(Map(key -> "x")))
        assert(e.getMessage.contains("restrict"), key)
      }
      assert(m.validateBatchConf(Map(ENGINE_SHARE_LEVEL.key -> "USER")) ===
        Map(ENGINE_SHARE_LEVEL.key -> "USER"))
      // batch applications may serve Spark Connect with TLS themselves
      val connectSslKey = "spark.ssl.connect.enabled"
      assert(m.validateBatchConf(Map(connectSslKey -> "true")) === Map(connectSslKey -> "true"))
    } finally {
      m.stop()
    }
  }
}
