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

import java.nio.ByteBuffer
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{DateDayVector, TimeStampVector, TinyIntVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class KyuubiFlightArrowUtilsSuite extends KyuubiFunSuite {

  test("populateRootFromRowSet writes columnar thrift without Seq materialization") {
    val emptyFields = util.Collections.emptyList[Field]()
    val schema = new Schema(util.Arrays.asList(
      new Field("flag", new FieldType(true, ArrowType.Bool.INSTANCE, null), emptyFields),
      new Field(
        "id",
        new FieldType(true, new ArrowType.Int(32, true), null),
        emptyFields),
      new Field(
        "score",
        new FieldType(
          true,
          new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
          null),
        emptyFields),
      new Field(
        "name",
        new FieldType(true, ArrowType.Utf8.INSTANCE, null),
        emptyFields)))

    val boolNulls = ByteBuffer.wrap(Array[Byte](0))
    val i32Nulls = ByteBuffer.wrap(Array[Byte](0))
    val doubleNulls = ByteBuffer.wrap(Array[Byte](0))
    val stringNulls = ByteBuffer.wrap(Array[Byte](0))

    val rowSet = new TRowSet()
    rowSet.addToColumns(TColumn.boolVal(
      new TBoolColumn(util.Arrays.asList(true, false), boolNulls)))
    rowSet.addToColumns(TColumn.i32Val(new TI32Column(util.Arrays.asList(1, 2), i32Nulls)))
    rowSet.addToColumns(TColumn.doubleVal(new TDoubleColumn(
      util.Arrays.asList(1.5d, 2.5d),
      doubleNulls)))
    rowSet.addToColumns(TColumn.stringVal(new TStringColumn(
      util.Arrays.asList("a", "b"),
      stringNulls)))

    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(schema, allocator)
    try {
      KyuubiFlightArrowUtils.populateRootFromRowSet(root, rowSet)
      assert(root.getRowCount === 2)
      assert(root.getVector(0).getObject(0) === true)
      assert(root.getVector(1).getObject(1).toString === "2")
      assert(root.getVector(2).getObject(0).toString === "1.5")
      assert(root.getVector(3).getObject(1).toString === "b")
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("isEmpty and isArrowRowSet helpers") {
    assert(KyuubiFlightArrowUtils.isEmpty(null))
    assert(KyuubiFlightArrowUtils.isEmpty(new TRowSet()))
    val arrow = new TRowSet()
    val binary = new TBinaryColumn()
    binary.addToValues(ByteBuffer.wrap(Array[Byte](1, 2, 3)))
    arrow.addToColumns(TColumn.binaryVal(binary))
    assert(KyuubiFlightArrowUtils.isArrowRowSet(arrow))
    assert(KyuubiFlightArrowUtils.arrowBatchBytes(arrow) === 3L)
    assert(!KyuubiFlightArrowUtils.isEmpty(arrow))
  }

  test("isEmpty is true for exhausted columnar thrift pages with schema columns") {
    // Matches TRowSetGenerator.toColumnBasedSet(Nil, schema): one empty TColumn per field.
    val emptyNulls = ByteBuffer.wrap(Array[Byte](0))
    val rowSet = new TRowSet()
    rowSet.setRows(new util.ArrayList[TRow]())
    rowSet.addToColumns(TColumn.i32Val(new TI32Column(new util.ArrayList[Integer](), emptyNulls)))
    rowSet.addToColumns(TColumn.stringVal(
      new TStringColumn(new util.ArrayList[String](), emptyNulls)))

    assert(rowSet.getColumnsSize === 2)
    assert(KyuubiFlightArrowUtils.rowCount(rowSet) === 0)
    assert(KyuubiFlightArrowUtils.isEmpty(rowSet))
    assert(!KyuubiFlightArrowUtils.isArrowRowSet(rowSet))
  }

  test("isEmpty is false when columnar thrift page has values") {
    val nulls = ByteBuffer.wrap(Array[Byte](0))
    val rowSet = new TRowSet()
    rowSet.addToColumns(TColumn.i32Val(new TI32Column(util.Arrays.asList(7), nulls)))
    assert(KyuubiFlightArrowUtils.rowCount(rowSet) === 1)
    assert(!KyuubiFlightArrowUtils.isEmpty(rowSet))
  }

  test("populateRootFromRowSet converts thrift string DATE/TIMESTAMP and widened TINYINT") {
    // Columnar thrift engines (JDBC, Hive, Trino) send DATE/TIMESTAMP as strings and the
    // JDBC engine sends TINYINT in an i16 column.
    val emptyFields = util.Collections.emptyList[Field]()
    val schema = new Schema(util.Arrays.asList(
      new Field("dt", new FieldType(true, new ArrowType.Date(DateUnit.DAY), null), emptyFields),
      new Field(
        "ts",
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, ""), null),
        emptyFields),
      new Field(
        "ts_msk",
        new FieldType(
          true,
          new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Europe/Moscow"),
          null),
        emptyFields),
      new Field("ti", new FieldType(true, new ArrowType.Int(8, true), null), emptyFields)))

    val nulls = ByteBuffer.wrap(Array[Byte](2)) // second row is NULL in every column
    val rowSet = new TRowSet()
    rowSet.addToColumns(TColumn.stringVal(
      new TStringColumn(util.Arrays.asList("2026-11-02", ""), nulls)))
    rowSet.addToColumns(TColumn.stringVal(
      new TStringColumn(util.Arrays.asList("2026-11-02 14:41:56.341143", ""), nulls)))
    rowSet.addToColumns(TColumn.stringVal(
      new TStringColumn(util.Arrays.asList("2026-11-02 14:41:56", ""), nulls)))
    rowSet.addToColumns(TColumn.i16Val(
      new TI16Column(util.Arrays.asList[java.lang.Short](7.toShort, 0.toShort), nulls)))

    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(schema, allocator)
    try {
      KyuubiFlightArrowUtils.populateRootFromRowSet(root, rowSet)
      assert(root.getRowCount === 2)

      val dt = root.getVector("dt").asInstanceOf[DateDayVector]
      assert(dt.get(0) === LocalDate.of(2026, 11, 2).toEpochDay.toInt)
      assert(dt.isNull(1))

      // zone-naive: wall clock stored as UTC regardless of the JVM default zone
      val ts = root.getVector("ts").asInstanceOf[TimeStampVector]
      val expectedNaive = LocalDateTime.of(2026, 11, 2, 14, 41, 56, 341143000)
        .toInstant(ZoneOffset.UTC)
      assert(ts.get(0) === expectedNaive.getEpochSecond * 1000000L + 341143L)
      assert(ts.isNull(1))

      // zone-aware: wall clock interpreted in the field's zone (MSK = UTC+3)
      val tsMsk = root.getVector("ts_msk").asInstanceOf[TimeStampVector]
      assert(tsMsk.get(0) ===
        LocalDateTime.of(2026, 11, 2, 11, 41, 56).toEpochSecond(ZoneOffset.UTC) * 1000000L)

      val ti = root.getVector("ti").asInstanceOf[TinyIntVector]
      assert(ti.get(0) === 7.toByte)
      assert(ti.isNull(1))
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("populateRootFromRowSet yields zero rows for exhausted columnar page") {
    val emptyFields = util.Collections.emptyList[Field]()
    val schema = new Schema(util.Arrays.asList(
      new Field("id", new FieldType(true, new ArrowType.Int(32, true), null), emptyFields),
      new Field("name", new FieldType(true, ArrowType.Utf8.INSTANCE, null), emptyFields)))

    val emptyNulls = ByteBuffer.wrap(Array[Byte](0))
    val rowSet = new TRowSet()
    rowSet.addToColumns(TColumn.i32Val(new TI32Column(new util.ArrayList[Integer](), emptyNulls)))
    rowSet.addToColumns(TColumn.stringVal(
      new TStringColumn(new util.ArrayList[String](), emptyNulls)))

    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(schema, allocator)
    try {
      KyuubiFlightArrowUtils.populateRootFromRowSet(root, rowSet)
      assert(root.getRowCount === 0)
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("schemaFromMetadata: Flight SQL column metadata from HS2 types and qualifiers") {
    def column(name: String, id: TTypeId, qualifiers: (String, Int)*): TColumnDesc = {
      val primitive = new TPrimitiveTypeEntry(id)
      if (qualifiers.nonEmpty) {
        primitive.setTypeQualifiers(new TTypeQualifiers(qualifiers.map { case (k, v) =>
          k -> TTypeQualifierValue.i32Value(v)
        }.toMap.asJava))
      }
      new TColumnDesc(
        name,
        new TTypeDesc(java.util.Collections.singletonList(TTypeEntry.primitiveEntry(primitive))),
        1)
    }
    val resp = new TGetResultSetMetadataResp(new TStatus(TStatusCode.SUCCESS_STATUS))
    resp.setSchema(new TTableSchema(java.util.Arrays.asList(
      column("d", TTypeId.DECIMAL_TYPE, "precision" -> 10, "scale" -> 2),
      column("v", TTypeId.VARCHAR_TYPE, "characterMaximumLength" -> 5),
      column("s", TTypeId.STRING_TYPE))))
    val schema = KyuubiFlightArrowUtils.schemaFromMetadata(resp)
    def meta(i: Int, key: String): String =
      schema.getFields.get(i).getMetadata.get("ARROW:FLIGHT:SQL:" + key)
    assert(meta(0, "TYPE_NAME") === "DECIMAL")
    assert(meta(0, "PRECISION") === "10")
    assert(meta(0, "SCALE") === "2")
    assert(meta(0, "IS_READ_ONLY") === "1")
    assert(meta(1, "TYPE_NAME") === "VARCHAR")
    assert(meta(1, "PRECISION") === "5")
    assert(meta(2, "PRECISION") === Int.MaxValue.toString)
    assert(meta(2, "IS_SEARCHABLE") === "1")
  }
}
