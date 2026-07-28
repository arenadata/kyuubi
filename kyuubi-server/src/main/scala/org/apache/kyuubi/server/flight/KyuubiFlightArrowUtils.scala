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
import java.nio.charset.StandardCharsets
import java.sql.Date
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.util.Text

import org.apache.kyuubi.jdbc.hive.KyuubiArrowQueryResultSet
import org.apache.kyuubi.jdbc.hive.arrow.ArrowUtils
import org.apache.kyuubi.server.trino.api.TrinoContext
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

object KyuubiFlightArrowUtils {

  def schemaFromMetadata(metadata: TGetResultSetMetadataResp): Schema = {
    val columns = Option(metadata).flatMap(m => Option(m.getSchema))
      .map(_.getColumns.asScala.toSeq)
      .getOrElse(Seq.empty)
    new Schema(columns.map { column =>
      val arrowType = Option(column.getTypeDesc)
        .flatMap(desc => Option(desc.getTypes).flatMap(_.asScala.headOption))
        .flatMap(typeEntry => primitiveArrowType(typeEntry))
        .getOrElse(ArrowType.Utf8.INSTANCE)
      new Field(
        column.getColumnName,
        new FieldType(true, arrowType, null),
        Collections.emptyList[Field]())
    }.asJava)
  }

  private def primitiveArrowType(typeEntry: TTypeEntry): Option[ArrowType] = {
    if (!typeEntry.isSetPrimitiveEntry) {
      None
    } else {
      val primitive = typeEntry.getPrimitiveEntry
      try {
        val attributes = KyuubiArrowQueryResultSet.getColumnAttributes(primitive)
        Some(ArrowUtils.toArrowType(primitive.getType, attributes))
      } catch {
        case _: Exception => Some(ArrowType.Utf8.INSTANCE)
      }
    }
  }

  def rowSetToRows(rowSet: TRowSet): Seq[Seq[AnyRef]] = {
    if (rowSet == null) {
      Seq.empty
    } else {
      TrinoContext.convertTRowSet(rowSet).asScala.map(_.asScala.toSeq).toSeq
    }
  }

  def populateRoot(root: VectorSchemaRoot, rows: Seq[Seq[AnyRef]]): Unit = {
    root.clear()
    root.allocateNew()
    val fields = root.getSchema.getFields.asScala
    val vectors = root.getFieldVectors.asScala
    vectors.zip(fields).zipWithIndex.foreach { case ((vector, field), columnIndex) =>
      rows.zipWithIndex.foreach { case (row, rowIndex) =>
        val value = if (columnIndex < row.length) row(columnIndex) else null
        setValue(vector, field.getType, rowIndex, value)
      }
      vector.setValueCount(rows.size)
    }
    root.setRowCount(rows.size)
  }

  private def setValue(
      vector: FieldVector,
      arrowType: ArrowType,
      rowIndex: Int,
      value: AnyRef): Unit = {
    if (value == null) {
      vector.setNull(rowIndex)
      return
    }

    arrowType match {
      case _: ArrowType.Utf8 =>
        vector.asInstanceOf[VarCharVector].setSafe(rowIndex, new Text(value.toString))
      case _: ArrowType.Binary =>
        vector.asInstanceOf[VarBinaryVector].setSafe(rowIndex, bytes(value))
      case _: ArrowType.Bool =>
        vector.asInstanceOf[BitVector].setSafe(rowIndex, if (value match {
          case b: java.lang.Boolean => b
          case s: String => s.toBoolean
          case _ => number(value).intValue() != 0
        }) 1 else 0)
      case intType: ArrowType.Int =>
        intType.getBitWidth match {
          case 8 => vector.asInstanceOf[TinyIntVector].setSafe(rowIndex, number(value).byteValue())
          case 16 => vector.asInstanceOf[SmallIntVector].setSafe(rowIndex, number(value).shortValue())
          case 32 => vector.asInstanceOf[IntVector].setSafe(rowIndex, number(value).intValue())
          case 64 => vector.asInstanceOf[BigIntVector].setSafe(rowIndex, number(value).longValue())
          case _ => throw new IllegalArgumentException(s"Unsupported integer width $intType")
        }
      case floatingPoint: ArrowType.FloatingPoint =>
        floatingPoint.getPrecision match {
          case FloatingPointPrecision.SINGLE =>
            vector.asInstanceOf[Float4Vector].setSafe(rowIndex, number(value).floatValue())
          case FloatingPointPrecision.DOUBLE =>
            vector.asInstanceOf[Float8Vector].setSafe(rowIndex, number(value).doubleValue())
          case _ => throw new IllegalArgumentException(s"Unsupported floating point type $arrowType")
        }
      case _: ArrowType.Decimal =>
        vector.asInstanceOf[DecimalVector].setSafe(rowIndex, new java.math.BigDecimal(value.toString))
      case _: ArrowType.Date =>
        val days = value match {
          case d: Date => (d.getTime / (24L * 60L * 60L * 1000L)).toInt
          case _ => number(value).intValue()
        }
        vector.asInstanceOf[DateDayVector].setSafe(rowIndex, days)
      case _ =>
        vector.asInstanceOf[VarCharVector].setSafe(rowIndex, new Text(value.toString))
    }
  }

  private def bytes(value: AnyRef): Array[Byte] = value match {
    case b: Array[Byte] => b
    case buffer: ByteBuffer =>
      val duplicate = buffer.duplicate()
      val result = new Array[Byte](duplicate.remaining())
      duplicate.get(result)
      result
    case text: Text => text.toString.getBytes(StandardCharsets.UTF_8)
    case other => other.toString.getBytes(StandardCharsets.UTF_8)
  }

  private def number(value: AnyRef): Number = value match {
    case n: Number => n
    case s: String => BigDecimal(s).bigDecimal
    case other => throw new IllegalArgumentException(s"Expected a number, got ${other.getClass}")
  }
}
