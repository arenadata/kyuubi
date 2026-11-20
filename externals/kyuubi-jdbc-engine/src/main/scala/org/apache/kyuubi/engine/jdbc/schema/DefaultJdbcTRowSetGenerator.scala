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
package org.apache.kyuubi.engine.jdbc.schema

import java.lang.{Short => JShort}
import java.nio.charset.StandardCharsets
import java.sql.Date
import java.sql.Types._
import java.time.LocalDateTime

import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.util.RowSetUtils.{formatDate, formatLocalDateTime}

class DefaultJdbcTRowSetGenerator extends JdbcTRowSetGenerator {

  override def toTColumn(rows: Seq[Seq[_]], ordinal: Int, sqlType: Int): TColumn =
    sqlType match {
      case BIT => toBitTColumn(rows, ordinal)
      case TINYINT => toTinyIntTColumn(rows, ordinal)
      case SMALLINT => toSmallIntTColumn(rows, ordinal)
      case INTEGER => toIntegerTColumn(rows, ordinal)
      case BIGINT => toBigIntTColumn(rows, ordinal)
      case REAL => toRealTColumn(rows, ordinal)
      case FLOAT => toFloatTColumn(rows, ordinal)
      case DOUBLE => toDoubleTColumn(rows, ordinal)
      case CHAR => toCharTColumn(rows, ordinal)
      case VARCHAR => toVarcharTColumn(rows, ordinal)
      case BOOLEAN => toBooleanTColumn(rows, ordinal)
      case _ => toDefaultTColumn(rows, ordinal, sqlType)
    }

  override def toTColumnValue(row: Seq[_], ordinal: Int, types: Seq[Column]): TColumnValue = {
    getColumnType(types, ordinal) match {
      case BIT => toBitTColumnValue(row, ordinal)
      case TINYINT => toTinyIntTColumnValue(row, ordinal)
      case SMALLINT => toSmallIntTColumnValue(row, ordinal)
      case INTEGER => toIntegerTColumnValue(row, ordinal)
      case BIGINT => toBigIntTColumnValue(row, ordinal)
      case REAL => toRealTColumnValue(row, ordinal)
      case FLOAT => toFloatTColumnValue(row, ordinal)
      case DOUBLE => toDoubleTColumnValue(row, ordinal)
      case CHAR => toCharTColumnValue(row, ordinal)
      case VARCHAR => toVarcharTColumnValue(row, ordinal)
      case BOOLEAN => toBooleanTColumnValue(row, ordinal)
      case otherType => toDefaultTColumnValue(row, ordinal, otherType)
    }
  }

  def toDefaultTColumn(rows: Seq[Seq[_]], ordinal: Int, sqlType: Int): TColumn =
    asStringTColumn(
      rows,
      ordinal,
      convertFunc = (row, ordinal) => toHiveString(row(ordinal), sqlType))

  def toBitTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asBooleanTColumn(rows, ordinal)

  def toBooleanTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asBooleanTColumn(rows, ordinal)

  // JDBC drivers return TINYINT/SMALLINT as Byte, Short or Integer (JDBC spec maps both to
  // Integer), so widen/narrow through Number instead of an unchecked cast to Short.
  def toTinyIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asShortTColumn(rows, ordinal, (row, i) => toJShort(row(i)))

  def toSmallIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asShortTColumn(rows, ordinal, (row, i) => toJShort(row(i)))

  def toIntegerTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asIntegerTColumn(rows, ordinal)

  def toBigIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asLongTColumn(rows, ordinal)

  def toRealTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asFloatTColumn(rows, ordinal)

  def toFloatTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asFloatTColumn(rows, ordinal)

  def toDoubleTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asDoubleTColumn(rows, ordinal)

  def toCharTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asStringTColumn(rows, ordinal)

  def toVarcharTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    asStringTColumn(rows, ordinal)

  // ==========================================================

  def toBitTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asBooleanTColumnValue(row, ordinal)

  def toBooleanTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asBooleanTColumnValue(row, ordinal)

  def toTinyIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asShortTColumnValue(row, ordinal, toJShort)

  def toSmallIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asShortTColumnValue(row, ordinal, toJShort)

  def toIntegerTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asIntegerTColumnValue(row, ordinal)

  def toBigIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asLongTColumnValue(row, ordinal)

  def toRealTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asFloatTColumnValue(row, ordinal)

  def toFloatTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asFloatTColumnValue(row, ordinal)

  def toDoubleTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asDoubleTColumnValue(row, ordinal)

  def toCharTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asStringTColumnValue(row, ordinal)

  def toVarcharTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    asStringTColumnValue(row, ordinal)

  def toDefaultTColumnValue(row: Seq[_], ordinal: Int, sqlType: Int): TColumnValue =
    asStringTColumnValue(row, ordinal, rawValue => toHiveString(rawValue, sqlType))

  protected def toJShort(value: Any): JShort = value match {
    case s: JShort => s
    case n: Number => JShort.valueOf(n.shortValue())
    case other => JShort.valueOf(other.toString.trim.toShort)
  }

  def toHiveString(data: Any, sqlType: Int): String =
    (data, sqlType) match {
      // BINARY/VARBINARY/LONGVARBINARY: drivers return byte[]; render like Hive (UTF-8 text)
      case (bytes: Array[Byte], _) => new String(bytes, StandardCharsets.UTF_8)
      case (date: Date, DATE) => formatDate(date)
      case (dateTime: LocalDateTime, TIMESTAMP) => formatLocalDateTime(dateTime)
      case (decimal: java.math.BigDecimal, DECIMAL) => decimal.toPlainString
      case (other, _) => other.toString
    }
}
