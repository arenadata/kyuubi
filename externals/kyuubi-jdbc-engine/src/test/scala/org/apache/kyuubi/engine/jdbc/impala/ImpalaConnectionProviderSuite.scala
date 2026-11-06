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

package org.apache.kyuubi.engine.jdbc.impala

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_JDBC_IMPALA_IMPERSONATION_ENABLED
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY

class ImpalaConnectionProviderSuite extends KyuubiFunSuite {

  import ImpalaConnectionProvider.{delegationProperties, IMPALA_DOAS_USER}

  test("impersonation is off by default") {
    val conf = KyuubiConf(false).set(KYUUBI_SESSION_USER_KEY, "alice")
    assert(delegationProperties(conf).isEmpty)
  }

  test("impersonation sets impala.doas.user to the session user") {
    val conf = KyuubiConf(false)
      .set(ENGINE_JDBC_IMPALA_IMPERSONATION_ENABLED, true)
      .set(KYUUBI_SESSION_USER_KEY, " alice ")
    assert(delegationProperties(conf) === Map(IMPALA_DOAS_USER -> "alice"))
  }

  test("no delegation without a session user") {
    val enabled = KyuubiConf(false).set(ENGINE_JDBC_IMPALA_IMPERSONATION_ENABLED, true)
    assert(delegationProperties(enabled).isEmpty)
    assert(delegationProperties(enabled.clone.set(KYUUBI_SESSION_USER_KEY, "  ")).isEmpty)
  }
}
