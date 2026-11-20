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

/**
 * Prepared statement bookkeeping that needs no engine: non-query statements are not executed
 * at prepare time, so these tests never launch an engine.
 */
class KyuubiFlightSqlPreparedStatementSuite extends WithFlightSqlServer with FlightSqlTestHelper {

  override protected val conf: KyuubiConf = KyuubiConf()

  private def as(user: String): HeaderCallOption = {
    val h = new FlightCallHeaders()
    h.insert("x-user-name", user)
    new HeaderCallOption(h)
  }

  test("preparing DDL returns an empty dataset schema without executing it") {
    withFlightSqlClient() { (_, sqlClient) =>
      val ps = sqlClient.prepare("CREATE TABLE kyuubi_flight_ps_never_created (a INT)", as("alice"))
      try {
        // empty schema -> JDBC drivers take the update path
        assert(ps.getResultSetSchema.getFields.isEmpty)
        assert(ps.getParameterSchema.getFields.isEmpty)
      } finally {
        ps.close(as("alice"))
      }
    }
  }

  test("a prepared statement can only be used by its owner") {
    withFlightSqlClient() { (_, sqlClient) =>
      val ps = sqlClient.prepare("DROP TABLE IF EXISTS kyuubi_flight_ps_owner", as("alice"))
      try {
        val e = intercept[FlightRuntimeException] {
          ps.executeUpdate(as("bob"))
        }
        assert(e.status().code() === FlightStatusCode.UNAUTHORIZED, e.getMessage)
      } finally {
        ps.close(as("alice"))
      }
    }
  }

  test("executing a closed prepared statement fails with NOT_FOUND") {
    withFlightSqlClient() { (_, sqlClient) =>
      val ps = sqlClient.prepare("DROP TABLE IF EXISTS kyuubi_flight_ps_closed", as("alice"))
      ps.close(as("alice"))
      val e = intercept[Exception] {
        ps.executeUpdate(as("alice"))
      }
      // the client may refuse locally after close; if it reaches the server it is NOT_FOUND
      e match {
        case f: FlightRuntimeException =>
          assert(f.status().code() === FlightStatusCode.NOT_FOUND, f.getMessage)
        case _ =>
      }
    }
  }
}
