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

import org.apache.arrow.flight.{CancelFlightInfoRequest, CancelStatus, FlightStatusCode}
import org.apache.arrow.flight.sql.CancelResult
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{FlightSqlTestHelper, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_FLIGHT_SQL_RESULT_IDLE_TIMEOUT

/**
 * A query result keeps its Kyuubi session (and engine session) open until DoGet; results that
 * are never fetched are released on cancel or after the result idle timeout.
 */
class KyuubiFlightSqlResultReleaseSuite extends WithFlightSqlServer with FlightSqlTestHelper {

  override protected val conf: KyuubiConf =
    KyuubiConf().set(FRONTEND_FLIGHT_SQL_RESULT_IDLE_TIMEOUT, 2000L)

  private def openSessions: Int = server.backendService.sessionManager.allSessions().size

  test("a result that is never fetched is released after the idle timeout") {
    withFlightSqlClient() { (_, sqlClient) =>
      val before = openSessions
      val info = sqlClient.execute("SELECT 1 AS value")
      assert(openSessions === before + 1)
      // 2 s timeout, 1 s sweep
      eventually(timeout(20.seconds), interval(500.millis)) {
        assert(openSessions === before)
      }
      assertFlightStatus(FlightStatusCode.NOT_FOUND) {
        readTicketRows(sqlClient, info.getEndpoints.get(0).getTicket)
      }
    }
  }

  test("a fetched result is released after DoGet and cannot be read twice") {
    withFlightSqlClient() { (_, sqlClient) =>
      val before = openSessions
      val info = sqlClient.execute("SELECT 1 AS value")
      val ticket = info.getEndpoints.get(0).getTicket
      assert(readTicketRows(sqlClient, ticket).size === 1)
      // released when DoGet returns on the server, just after the client saw the end
      eventually(timeout(10.seconds), interval(100.millis)) {
        assert(openSessions === before)
      }
      assertFlightStatus(FlightStatusCode.NOT_FOUND) {
        readTicketRows(sqlClient, ticket)
      }
    }
  }

  test("CancelFlightInfo releases a result that is not fetched") {
    withFlightSqlClient() { (_, sqlClient) =>
      val before = openSessions
      val info = sqlClient.execute("SELECT 1 AS value")
      assert(sqlClient.cancelFlightInfo(new CancelFlightInfoRequest(info)).getStatus ===
        CancelStatus.CANCELLED)
      assert(openSessions === before)
      assertFlightStatus(FlightStatusCode.NOT_FOUND) {
        readTicketRows(sqlClient, info.getEndpoints.get(0).getTicket)
      }
      assert(sqlClient.cancelFlightInfo(new CancelFlightInfoRequest(info)).getStatus ===
        CancelStatus.NOT_CANCELLABLE)
    }
  }

  test("legacy CancelQuery releases a result that is not fetched") {
    withFlightSqlClient() { (_, sqlClient) =>
      val before = openSessions
      val info = sqlClient.execute("SELECT 1 AS value")
      // deprecated in Arrow, still sent by older clients
      assert(sqlClient.cancelQuery(info) === CancelResult.CANCELLED)
      assert(openSessions === before)
    }
  }
}
