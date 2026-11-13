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
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.util.TableRef

import org.apache.kyuubi.{FlightSqlTestHelper, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}

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
      val ps = sqlClient.prepare(
        // a data source table: the test engine runs without Hive support
        "CREATE TABLE IF NOT EXISTS kyuubi_flight_ps_update (a INT) USING parquet")
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

  test("result schemas carry Flight SQL column metadata, also in the stream") {
    withFlightSqlClient() { (_, sqlClient) =>
      val info = sqlClient.execute("SELECT CAST(1.5 AS DECIMAL(10,2)) AS d, 'x' AS s")
      val fields = info.getSchemaOptional.get().getFields
      assert(fields.get(0).getMetadata.get("ARROW:FLIGHT:SQL:TYPE_NAME") === "DECIMAL")
      assert(fields.get(0).getMetadata.get("ARROW:FLIGHT:SQL:PRECISION") === "10")
      assert(fields.get(0).getMetadata.get("ARROW:FLIGHT:SQL:SCALE") === "2")
      assert(fields.get(1).getMetadata.get("ARROW:FLIGHT:SQL:TYPE_NAME") === "STRING")
      val stream = sqlClient.getStream(info.getEndpoints.get(0).getTicket)
      try {
        while (stream.next()) {}
        assert(stream.getSchema === info.getSchemaOptional.get())
      } finally {
        stream.close()
      }
    }
  }

  test("prepared update then its result request runs the statement once") {
    withFlightSqlClient() { (_, sqlClient) =>
      // the Arrow Flight SQL JDBC driver runs an update via DoPut and then asks for its
      // result; running the DDL again would fail with "already exists"
      val ps = sqlClient.prepare("CREATE TABLE kyuubi_flight_ps_once (a INT) USING parquet")
      try {
        assert(ps.executeUpdate() === 0L)
        val info = ps.execute()
        assert(info.getSchemaOptional.get().getFields.isEmpty)
        assert(readAllRows(sqlClient, info).isEmpty)
        val rows = executeAndCollect(sqlClient, "SHOW TABLES LIKE 'kyuubi_flight_ps_once'")
        assert(rows.size === 1)
      } finally {
        ps.close()
        sqlClient.executeUpdate("DROP TABLE IF EXISTS kyuubi_flight_ps_once")
      }
    }
  }

  test("key metadata requests answer with the Flight SQL schemas instead of UNIMPLEMENTED") {
    withFlightSqlClient() { (_, sqlClient) =>
      val table = TableRef.of(null, "default", "kyuubi_flight_keys")
      val requests = Seq(
        Schemas.GET_PRIMARY_KEYS_SCHEMA -> (() => sqlClient.getPrimaryKeys(table)),
        Schemas.GET_IMPORTED_KEYS_SCHEMA -> (() => sqlClient.getImportedKeys(table)),
        Schemas.GET_EXPORTED_KEYS_SCHEMA -> (() => sqlClient.getExportedKeys(table)),
        Schemas.GET_CROSS_REFERENCE_SCHEMA -> (() => sqlClient.getCrossReference(table, table)))
      requests.foreach { case (schema, request) =>
        val info = request()
        assert(info.getSchemaOptional.get() === schema)
        // the Spark engine declares no keys (or does not support the request at all)
        assert(readAllRows(sqlClient, info).isEmpty)
      }
    }
  }

  private def openOperations: Long =
    MetricsSystem.counterValue(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN).getOrElse(0L)

  test("failed queries leave no open operation, also when the result type is unsupported") {
    withFlightSqlClient() { (_, sqlClient) =>
      val before = openOperations
      intercept[FlightRuntimeException] {
        sqlClient.execute("SELECT * FROM kyuubi_flight_missing_table_open")
      }
      // ARRAY results have no Flight SQL mapping yet: the schema conversion fails after the
      // statement ran
      val e = intercept[FlightRuntimeException] {
        sqlClient.execute("SELECT array(1, 2) AS a")
      }
      assert(Option(e.getMessage).getOrElse("").contains("Unsupported"), e.getMessage)
      assert(openOperations === before)
    }
  }

  test("a query sent through the update path reports 0 rows") {
    withFlightSqlClient() { (_, sqlClient) =>
      // only the first rows are read to look for an update count
      assert(sqlClient.executeUpdate("SELECT id FROM range(1000000)") === 0L)
      // the statement ran; a result type without a Flight SQL mapping only means no count
      assert(sqlClient.executeUpdate("SELECT array(1, 2) AS a") === 0L)
    }
  }
}
