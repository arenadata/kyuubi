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

package org.apache.kyuubi.operation.flight

import org.apache.arrow.flight.{FlightCallHeaders, FlightRuntimeException, HeaderCallOption}

import org.apache.kyuubi.{FlightSqlTestHelper, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

/**
 * `kyuubi.*` call headers reach session validation. The restrict list rejects the session
 * before any engine is launched, so this needs no engine on the test machine.
 */
class KyuubiFlightSqlSessionConfSuite extends WithFlightSqlServer with FlightSqlTestHelper {

  private val camelCaseKey = ENGINE_JDBC_CONNECTION_PROPAGATECREDENTIAL.key

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(SESSION_CONF_RESTRICT_LIST.key, s"${ENGINE_PROFILE.key},$camelCaseKey")

  private def headers(kv: (String, String)*): HeaderCallOption = {
    val h = new FlightCallHeaders()
    kv.foreach { case (k, v) => h.insert(k, v) }
    new HeaderCallOption(h)
  }

  test("kyuubi.engine.profile call header is applied to the Flight SQL session") {
    withFlightSqlClient() { (_, sqlClient) =>
      val e = intercept[FlightRuntimeException] {
        sqlClient.execute("SELECT 1", headers(ENGINE_PROFILE.key -> "impala"))
      }
      assert(e.getMessage.contains(s"${ENGINE_PROFILE.key} is a restrict key"), e.getMessage)
    }
  }

  test("lower-cased header name is matched to the camelCase config key") {
    withFlightSqlClient() { (_, sqlClient) =>
      val e = intercept[FlightRuntimeException] {
        sqlClient.execute("SELECT 1", headers(camelCaseKey.toLowerCase -> "true"))
      }
      assert(e.getMessage.contains(s"$camelCaseKey is a restrict key"), e.getMessage)
    }
  }
}
