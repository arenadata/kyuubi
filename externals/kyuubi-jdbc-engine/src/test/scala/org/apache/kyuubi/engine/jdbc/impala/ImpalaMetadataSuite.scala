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

import java.sql.Types

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.jdbc.impala.ImpalaMetadata._
import org.apache.kyuubi.engine.jdbc.schema.Row
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.shaded.thrift.protocol.TBinaryProtocol
import org.apache.kyuubi.shaded.thrift.transport.TMemoryBuffer

/** Canned SHOW/DESCRIBE output in the shape Impala 4.5 returns it. */
class ImpalaMetadataSuite extends KyuubiFunSuite {

  private def names(values: String*): Seq[Map[String, String]] = values.map(v => Map("name" -> v))

  private class FakeImpala(showViewsFails: Boolean = false) {
    val executed = ArrayBuffer[String]()

    def run(sql: String): Seq[Map[String, String]] = {
      executed += sql
      sql match {
        case "SHOW DATABASES" =>
          Seq(
            Map("name" -> "_impala_builtins", "comment" -> "System database"),
            Map("name" -> "default", "comment" -> "Default Hive database"),
            Map("name" -> "sales_db", "comment" -> ""),
            Map("name" -> "vdmitriev", "comment" -> ""))
        case "SHOW TABLES IN `_impala_builtins`" => Seq.empty
        case "SHOW TABLES IN `default`" => names("t_default")
        case "SHOW TABLES IN `sales_db`" => names("store_sales", "store_view", "items")
        case "SHOW TABLES IN `vdmitriev`" => names("table_parquet", "events")
        case v if v.startsWith("SHOW VIEWS IN") && showViewsFails =>
          throw new java.sql.SQLException("ParseException: Syntax error")
        case "SHOW VIEWS IN `sales_db`" => names("store_view")
        case v if v.startsWith("SHOW VIEWS IN") => Seq.empty
        case "DESCRIBE `sales_db`.`store_sales`" =>
          Seq(
            Map("name" -> "ss_item_sk", "type" -> "bigint", "comment" -> ""),
            Map("name" -> "ss_list_price", "type" -> "decimal(7,2)", "comment" -> "list price"),
            Map("name" -> "ss_sold_date_sk", "type" -> "bigint", "comment" -> ""))
        case "DESCRIBE `default`.`t_default`" | "DESCRIBE `sales_db`.`items`" =>
          Seq(Map("name" -> "id", "type" -> "int", "comment" -> ""))
        case "DESCRIBE `sales_db`.`store_view`" =>
          Seq(Map("name" -> "tags", "type" -> "array<string>", "comment" -> ""))
        case "DESCRIBE `vdmitriev`.`table_parquet`" =>
          Seq(
            Map("name" -> "id", "type" -> "int", "comment" -> ""),
            Map("name" -> "data", "type" -> "string", "comment" -> ""))
        case "DESCRIBE `vdmitriev`.`events`" => // Iceberg adds a nullable column
          Seq(
            Map("name" -> "device_id", "type" -> "bigint", "comment" -> "", "nullable" -> "false"),
            Map("name" -> "country", "type" -> "string", "comment" -> "", "nullable" -> "true"))
        case other => fail(s"unexpected statement: $other")
      }
    }

    val metadata = new ImpalaMetadata(run)
  }

  private def col(row: Row, i: Int): Any = row.values(i)

  test("catalogs are empty, schemas follow SHOW DATABASES with JDBC patterns") {
    val impala = new FakeImpala
    assert(impala.metadata.catalogs().isEmpty)
    assert(impala.metadata.schemas(null).map(col(_, 0)) ===
      Seq("_impala_builtins", "default", "sales_db", "vdmitriev"))
    assert(impala.metadata.schemas("%").size === 4)
    assert(impala.metadata.schemas("SALES%").map(col(_, 0)) === Seq("sales_db"))
    assert(impala.metadata.schemas("vdmitr_ev").map(col(_, 0)) === Seq("vdmitriev"))
    assert(impala.metadata.schemas("*dm*").map(col(_, 0)) === Seq("vdmitriev"))
    // TABLE_SCHEM, TABLE_CATALOG
    assert(impala.metadata.schemas("default").head.values === List("default", null))
  }

  test("tables carry schema, name and TABLE/VIEW type in JDBC column order") {
    val impala = new FakeImpala
    val rows = impala.metadata.tables("sales_db", "%", Seq.empty)
    assert(rows.map(r => (col(r, 1), col(r, 2), col(r, 3))) === Seq(
      ("sales_db", "store_sales", TABLE),
      ("sales_db", "store_view", VIEW),
      ("sales_db", "items", TABLE)))
    rows.foreach(r => assert(r.values.size === TABLES_COLUMNS.size))

    assert(impala.metadata.tables("sales_db", "store%", Seq("VIEW")).map(col(_, 2)) ===
      Seq("store_view"))
    assert(impala.metadata.tables("sales_db", null, Seq("table")).map(col(_, 2)) ===
      Seq("store_sales", "items"))
    // all databases
    assert(impala.metadata.tables(null, "%", Seq.empty).map(col(_, 2)).toSet ===
      Set("t_default", "store_sales", "store_view", "items", "table_parquet", "events"))
    // identifiers are back-quoted
    assert(impala.executed.contains("SHOW TABLES IN `sales_db`"))
  }

  test("tables fall back to TABLE when SHOW VIEWS is not supported") {
    val impala = new FakeImpala(showViewsFails = true)
    assert(impala.metadata.tables("sales_db", "%", Seq.empty).map(col(_, 3)).toSet === Set(TABLE))
  }

  test("columns map Impala types to JDBC types with position, nullability and remarks") {
    val impala = new FakeImpala
    val rows = impala.metadata.columns("sales_db", "store_sales", "%")
    rows.foreach(r => assert(r.values.size === COLUMNS_COLUMNS.size))
    val price = rows(1)
    assert(col(price, 1) === "sales_db")
    assert(col(price, 2) === "store_sales")
    assert(col(price, 3) === "ss_list_price")
    assert(col(price, 4) === Types.DECIMAL)
    assert(col(price, 5) === "DECIMAL(7,2)")
    assert(col(price, 6) === 7) // COLUMN_SIZE
    assert(col(price, 8) === 2) // DECIMAL_DIGITS
    assert(col(price, 11) === "list price") // REMARKS
    assert(col(price, 16) === 2) // ORDINAL_POSITION
    assert(col(price, 17) === "YES")
    // partition column is part of DESCRIBE output and keeps its position
    assert(col(rows(2), 3) === "ss_sold_date_sk")
    assert(col(rows(2), 16) === 3)

    val events = impala.metadata.columns("vdmitriev", "events", null)
    assert(col(events.head, 10) === java.sql.DatabaseMetaData.columnNoNulls)
    assert(col(events.head, 17) === "NO")
    assert(col(events(1), 17) === "YES")

    assert(impala.metadata.columns("vdmitriev", "%", "d%").map(r => (col(r, 2), col(r, 3))) ===
      Seq(("table_parquet", "data"), ("events", "device_id")))
  }

  test("Impala type strings map to JDBC types") {
    assert(toJdbcType("int") === JdbcType(Types.INTEGER, "INT", Some(10), Some(0), Some(10)))
    assert(toJdbcType("string").sqlType === Types.VARCHAR)
    assert(toJdbcType("varchar(10)") === JdbcType(Types.VARCHAR, "VARCHAR(10)", Some(10)))
    assert(toJdbcType("char(3)") === JdbcType(Types.CHAR, "CHAR(3)", Some(3)))
    assert(toJdbcType("decimal(18, 0)") ===
      JdbcType(Types.DECIMAL, "DECIMAL(18,0)", Some(18), Some(0), Some(10)))
    assert(toJdbcType("decimal").typeName === "DECIMAL(9,0)")
    assert(toJdbcType("timestamp").sqlType === Types.TIMESTAMP)
    assert(toJdbcType("date").sqlType === Types.DATE)
    assert(toJdbcType("boolean").sqlType === Types.BOOLEAN)
    assert(toJdbcType("tinyint").sqlType === Types.TINYINT)
    assert(toJdbcType("float").sqlType === Types.FLOAT)
    assert(toJdbcType("binary").sqlType === Types.BINARY)
    assert(toJdbcType("array<int>").sqlType === Types.ARRAY)
    assert(toJdbcType("map<string,int>").sqlType === Types.JAVA_OBJECT)
    assert(toJdbcType("struct<\n  a:int,\n  b:string\n>") ===
      JdbcType(Types.STRUCT, "STRUCT< A:INT, B:STRING >"))
  }

  test("metadata rows serialize through the Impala row generator") {
    val impala = new FakeImpala
    val generator = new ImpalaTRowSetGenerator
    Seq(
      (impala.metadata.schemas(null), SCHEMAS_COLUMNS),
      (impala.metadata.tables(null, "%", Seq.empty), TABLES_COLUMNS),
      (impala.metadata.columns(null, "%", "%"), COLUMNS_COLUMNS),
      (TABLE_TYPES_ROWS, TABLE_TYPES_COLUMNS)).foreach { case (rows, columns) =>
      val rowSet = generator.toTRowSet(
        rows.map(_.values),
        columns,
        TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
      rowSet.write(new TBinaryProtocol(new TMemoryBuffer(1024)))
      assert(rowSet.getColumnsSize === columns.size)
    }
  }
}
