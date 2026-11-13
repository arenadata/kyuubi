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

package org.apache.kyuubi.server.flight

import java.io.ByteArrayInputStream
import java.nio.channels.Channels
import java.sql.Types

import scala.collection.JavaConverters._

import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}

import org.apache.kyuubi.{FlightSqlTestHelper, KyuubiFunSuite}
import org.apache.kyuubi.server.flight.KyuubiFlightMetadata._

class KyuubiFlightMetadataSuite extends KyuubiFunSuite {

  private def withRoot[T](schema: Schema)(f: VectorSchemaRoot => T): T = {
    FlightSqlTestHelper.ensureArrowUnsafeAllocator()
    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(schema, allocator)
    try f(root)
    finally {
      root.close()
      allocator.close()
    }
  }

  private def strings(root: VectorSchemaRoot, field: String): Seq[String] = {
    val v = root.getVector(field)
    (0 until root.getRowCount).map(i => if (v.isNull(i)) null else v.getObject(i).toString)
  }

  private def readSchema(bytes: Array[Byte]): Schema =
    MessageSerializer.deserializeSchema(
      new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))))

  test("GetSchemas: JDBC TABLE_SCHEM/TABLE_CATALOG map to catalog_name/db_schema_name") {
    // JDBC column order is the reverse of Flight SQL's
    val rows = Seq(
      Map[String, AnyRef]("table_schem" -> "default", "table_catalog" -> null),
      Map[String, AnyRef]("table_schem" -> "vdmitriev", "table_catalog" -> "spark_catalog"))
    withRoot(Schemas.GET_SCHEMAS_SCHEMA) { root =>
      fill(root, rows, SCHEMAS_SOURCES)
      assert(root.getRowCount === 2)
      assert(strings(root, "db_schema_name") === Seq("default", "vdmitriev"))
      assert(strings(root, "catalog_name") === Seq(null, "spark_catalog"))
    }
  }

  test("GetCatalogs and GetTableTypes are looked up by name, case-insensitively") {
    withRoot(Schemas.GET_CATALOGS_SCHEMA) { root =>
      fill(root, Seq(Map[String, AnyRef]("table_cat" -> "spark_catalog")), CATALOGS_SOURCES)
      assert(strings(root, "catalog_name") === Seq("spark_catalog"))
    }
    withRoot(Schemas.GET_TABLE_TYPES_SCHEMA) { root =>
      fill(
        root,
        Seq(
          Map[String, AnyRef]("table_type" -> "TABLE"),
          Map[String, AnyRef]("table_type" -> "VIEW")),
        TABLE_TYPES_SOURCES)
      assert(strings(root, "table_type") === Seq("TABLE", "VIEW"))
    }
    assert(lookup(Map("table_schem" -> "x"), Seq("TABLE_SCHEM")).contains("x"))
  }

  test("GetTables without schema: JDBC columns by name, required fields never null") {
    val rows = Seq(Map[String, AnyRef](
      "table_cat" -> null,
      "table_schem" -> "vdmitriev",
      "table_name" -> "table_parquet",
      "table_type" -> "TABLE",
      "remarks" -> "a comment"))
    withRoot(Schemas.GET_TABLES_SCHEMA_NO_SCHEMA) { root =>
      fill(root, rows, TABLES_SOURCES)
      assert(strings(root, "catalog_name") === Seq(null))
      assert(strings(root, "db_schema_name") === Seq("vdmitriev"))
      assert(strings(root, "table_name") === Seq("table_parquet"))
      assert(strings(root, "table_type") === Seq("TABLE"))
    }
    // non-nullable Flight field without a source value -> ""
    withRoot(Schemas.GET_TABLES_SCHEMA_NO_SCHEMA) { root =>
      fill(root, Seq(Map[String, AnyRef]("table_name" -> "t")), TABLES_SOURCES)
      assert(strings(root, "table_type") === Seq(""))
    }
  }

  test("GetTables with schema: table_schema is an IPC Arrow schema built from GetColumns") {
    val columnRows = Seq(
      // deliberately out of ordinal order
      Map[String, AnyRef](
        "table_cat" -> null,
        "table_schem" -> "sales",
        "table_name" -> "store_sales",
        "column_name" -> "price",
        "data_type" -> Int.box(Types.DECIMAL),
        "column_size" -> Int.box(7),
        "decimal_digits" -> Int.box(2),
        "nullable" -> Int.box(1),
        "ordinal_position" -> Int.box(2)),
      Map[String, AnyRef](
        "table_cat" -> null,
        "table_schem" -> "sales",
        "table_name" -> "store_sales",
        "column_name" -> "id",
        "data_type" -> Int.box(Types.BIGINT),
        "nullable" -> Int.box(0),
        "ordinal_position" -> Int.box(1)))
    val schemas = tableSchemas(columnRows)
    assert(schemas.keySet === Set(("", "sales", "store_sales")))

    val tableRows = Seq(
      Map[String, AnyRef](
        "table_schem" -> "sales",
        "table_name" -> "store_sales",
        "table_type" -> "TABLE"),
      Map[String, AnyRef](
        "table_schem" -> "sales",
        "table_name" -> "no_columns",
        "table_type" -> "VIEW"))
    withRoot(Schemas.GET_TABLES_SCHEMA) { root =>
      fill(root, tableRows, TABLES_SOURCES, schemas)
      val bytes = root.getVector(TABLE_SCHEMA_FIELD).asInstanceOf[VarBinaryVector]
      val schema = readSchema(bytes.get(0))
      assert(schema.getFields.asScala.map(_.getName) === Seq("id", "price"))
      assert(schema.findField("id").getType === new ArrowType.Int(64, true))
      assert(!schema.findField("id").isNullable)
      assert(schema.findField("price").getType === new ArrowType.Decimal(7, 2, 128))
      assert(readSchema(bytes.get(1)).getFields.isEmpty)
    }
  }

  test("JDBC types map to Arrow types") {
    assert(toArrowType(Types.INTEGER, None, None) === new ArrowType.Int(32, true))
    assert(toArrowType(Types.VARCHAR, Some(Int.MaxValue), None) === ArrowType.Utf8.INSTANCE)
    assert(toArrowType(Types.DECIMAL, None, None) === new ArrowType.Decimal(38, 0, 128))
    assert(toArrowType(Types.BOOLEAN, None, None) === ArrowType.Bool.INSTANCE)
    assert(toArrowType(Types.BINARY, None, None) === ArrowType.Binary.INSTANCE)
    assert(toArrowType(Types.ARRAY, None, None) === ArrowType.Utf8.INSTANCE)
  }

  test("GetPrimaryKeys: Hive's KEQ_SEQ fills key_sequence") {
    val rows = Seq(Map[String, AnyRef](
      "table_cat" -> null,
      "table_schem" -> "sales",
      "table_name" -> "orders",
      "column_name" -> "id",
      "keq_seq" -> Int.box(1),
      "pk_name" -> "orders_pk"))
    withRoot(Schemas.GET_PRIMARY_KEYS_SCHEMA) { root =>
      fill(root, rows, PRIMARY_KEYS_SOURCES)
      assert(root.getRowCount === 1)
      assert(strings(root, "catalog_name") === Seq(null))
      assert(strings(root, "db_schema_name") === Seq("sales"))
      assert(strings(root, "table_name") === Seq("orders"))
      assert(strings(root, "column_name") === Seq("id"))
      assert(strings(root, "key_name") === Seq("orders_pk"))
      assert(strings(root, "key_sequence") === Seq("1"))
    }
  }

  test("foreign keys: JDBC columns map by name, missing rules default to NO ACTION") {
    val rows = Seq(Map[String, AnyRef](
      "pktable_schem" -> "sales",
      "pktable_name" -> "customers",
      "pkcolumn_name" -> "id",
      "fktable_schem" -> "sales",
      "fktable_name" -> "orders",
      "fkcolumn_name" -> "customer_id",
      "key_seq" -> "1",
      "update_rule" -> Short.box(0),
      "fk_name" -> "orders_customer_fk",
      "pk_name" -> "customers_pk"))
    Seq(
      Schemas.GET_IMPORTED_KEYS_SCHEMA,
      Schemas.GET_EXPORTED_KEYS_SCHEMA,
      Schemas.GET_CROSS_REFERENCE_SCHEMA).foreach { schema =>
      withRoot(schema) { root =>
        fill(root, rows, FOREIGN_KEYS_SOURCES)
        assert(root.getRowCount === 1)
        assert(strings(root, "pk_catalog_name") === Seq(null))
        assert(strings(root, "pk_table_name") === Seq("customers"))
        assert(strings(root, "pk_column_name") === Seq("id"))
        assert(strings(root, "fk_db_schema_name") === Seq("sales"))
        assert(strings(root, "fk_table_name") === Seq("orders"))
        assert(strings(root, "fk_column_name") === Seq("customer_id"))
        assert(strings(root, "key_sequence") === Seq("1"))
        assert(strings(root, "fk_key_name") === Seq("orders_customer_fk"))
        assert(strings(root, "pk_key_name") === Seq("customers_pk"))
        assert(strings(root, "update_rule") === Seq("0")) // CASCADE
        assert(strings(root, "delete_rule") ===
          Seq(java.sql.DatabaseMetaData.importedKeyNoAction.toString))
      }
    }
  }

  test("GetTables with schema: fields carry Flight SQL column metadata") {
    val columnRows = Seq(
      Map[String, AnyRef](
        "table_schem" -> "sales",
        "table_name" -> "t",
        "column_name" -> "price",
        "data_type" -> Int.box(Types.DECIMAL),
        "type_name" -> "decimal(10,2)",
        "column_size" -> Int.box(10),
        "decimal_digits" -> Int.box(2),
        "ordinal_position" -> Int.box(1)),
      Map[String, AnyRef](
        "table_schem" -> "sales",
        "table_name" -> "t",
        "column_name" -> "code",
        "data_type" -> Int.box(Types.VARCHAR),
        "type_name" -> "varchar(5)",
        "column_size" -> Int.box(5),
        "ordinal_position" -> Int.box(2)),
      Map[String, AnyRef](
        "table_schem" -> "sales",
        "table_name" -> "t",
        "column_name" -> "id",
        // no TYPE_NAME: the JDBC type name
        "data_type" -> Int.box(Types.BIGINT),
        "ordinal_position" -> Int.box(3)))
    val schema = readSchema(tableSchemas(columnRows)(("", "sales", "t")))
    def meta(i: Int, key: String): String =
      schema.getFields.get(i).getMetadata.get("ARROW:FLIGHT:SQL:" + key)
    assert(meta(0, "TYPE_NAME") === "DECIMAL")
    assert(meta(0, "PRECISION") === "10")
    assert(meta(0, "SCALE") === "2")
    assert(meta(0, "SCHEMA_NAME") === "sales")
    assert(meta(0, "TABLE_NAME") === "t")
    assert(meta(0, "IS_READ_ONLY") === "0")
    assert(meta(1, "TYPE_NAME") === "VARCHAR")
    assert(meta(1, "PRECISION") === "5")
    assert(meta(1, "IS_CASE_SENSITIVE") === "1")
    assert(meta(2, "TYPE_NAME") === "BIGINT")
  }
}
