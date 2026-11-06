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
}
