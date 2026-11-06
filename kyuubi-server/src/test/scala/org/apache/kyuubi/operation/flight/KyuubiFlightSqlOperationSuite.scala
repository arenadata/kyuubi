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

import org.apache.arrow.flight.{FlightRuntimeException, FlightStatusCode}

import org.apache.kyuubi.{FlightSqlTestHelper, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf

class KyuubiFlightSqlOperationSuite extends WithFlightSqlServer with FlightSqlTestHelper {

  override protected val conf: KyuubiConf = KyuubiConf()

  test("multi-column query returns schema and rows") {
    withFlightSqlClient() { (_, sqlClient) =>
      val info = sqlClient.execute("SELECT 1 AS id, 'kyuubi' AS name")
      assert(info.getSchemaOptional.get().getFields.size() === 2)
      assert(info.getSchemaOptional.get().getFields.get(0).getName.equalsIgnoreCase("id"))
      assert(info.getSchemaOptional.get().getFields.get(1).getName.equalsIgnoreCase("name"))
      val rows = readAllRows(sqlClient, info)
      assert(rows.size === 1)
      assert(rows.head(0).toString === "1")
      assert(rows.head(1).toString === "kyuubi")
    }
  }

  test("failed query reports the engine error, not a result metadata failure") {
    withFlightSqlClient() { (_, sqlClient) =>
      val e = intercept[FlightRuntimeException] {
        sqlClient.execute("SELECT * FROM kyuubi_flight_missing_table_xyz")
      }
      assert(e.status().code() === FlightStatusCode.INTERNAL)
      val message = Option(e.getMessage).getOrElse("")
      assert(message.contains("kyuubi_flight_missing_table_xyz"), message)
    }
  }

  test("prepared query: schema at prepare time, rows on each execution") {
    withFlightSqlClient() { (_, sqlClient) =>
      val ps = sqlClient.prepare("SELECT 1 AS id, 'kyuubi' AS name")
      try {
        val schema = ps.getResultSetSchema
        assert(schema.getFields.size() === 2)
        assert(schema.getFields.get(0).getName.equalsIgnoreCase("id"))
        assert(ps.getParameterSchema.getFields.isEmpty)
        // first execution is served by the result computed at prepare time,
        // the second one re-runs the query
        (1 to 2).foreach { _ =>
          val rows = readAllRows(sqlClient, ps.execute())
          assert(rows.size === 1)
          assert(rows.head(1).toString === "kyuubi")
        }
      } finally {
        ps.close()
      }
    }
  }

  test("prepared and plain update statements run on execution") {
    withFlightSqlClient() { (_, sqlClient) =>
      val ps = sqlClient.prepare("CREATE TABLE IF NOT EXISTS kyuubi_flight_ps_update (a INT)")
      try {
        assert(ps.getResultSetSchema.getFields.isEmpty)
        ps.executeUpdate()
        val rows = executeAndCollect(sqlClient, "SHOW TABLES LIKE 'kyuubi_flight_ps_update'")
        assert(rows.nonEmpty)
      } finally {
        ps.close()
        sqlClient.executeUpdate("DROP TABLE IF EXISTS kyuubi_flight_ps_update")
      }
    }
  }
}
