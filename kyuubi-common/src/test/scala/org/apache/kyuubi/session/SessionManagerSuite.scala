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
        "hadoop.security.credstore.java-keystore-provider.password-file").foreach { key =>
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
      val e = intercept[KyuubiSQLException] {
        m.validateBatchConf(Map("hadoop.security.credential.provider.path" -> "vault://x:8200/y"))
      }
      assert(e.getMessage.contains("restrict"))
    } finally {
      m.stop()
    }
  }
}
