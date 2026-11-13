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

import org.apache.arrow.flight.{FlightCallHeaders, FlightRuntimeException, FlightStatusCode, HeaderCallOption}

import org.apache.kyuubi.{FlightSqlTestHelper, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_FLIGHT_SQL_TOKEN_TTL

/**
 * Idle prepared statements expire without another prepare call. A non-query statement is not
 * executed at prepare time, so no engine is launched.
 */
class KyuubiFlightSqlPreparedStatementExpirySuite
  extends WithFlightSqlServer with FlightSqlTestHelper {

  override protected val conf: KyuubiConf = KyuubiConf().set(FRONTEND_FLIGHT_SQL_TOKEN_TTL, 2000L)

  private val alice: HeaderCallOption = {
    val h = new FlightCallHeaders()
    h.insert("x-user-name", "alice")
    new HeaderCallOption(h)
  }

  test("an abandoned prepared statement expires without another prepare") {
    withFlightSqlClient() { (_, sqlClient) =>
      val ps = sqlClient.prepare("DROP TABLE IF EXISTS kyuubi_flight_ps_abandoned", alice)
      // Not closed by the client. Every use of the handle refreshes its idle time (and executing
      // it would run the statement), so wait past the 2 s timeout plus one 1 s sweep first.
      Thread.sleep(4500)
      val e = intercept[FlightRuntimeException](ps.executeUpdate(alice))
      assert(e.status().code() === FlightStatusCode.NOT_FOUND, e.getMessage)
    }
  }
}
