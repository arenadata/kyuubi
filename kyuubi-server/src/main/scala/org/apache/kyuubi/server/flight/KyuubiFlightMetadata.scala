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

import java.nio.charset.StandardCharsets
import java.sql.Types
import java.util.{Collections, Locale}

import scala.collection.JavaConverters._

import org.apache.arrow.vector.{BaseIntVector, FieldVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.util.Text

/**
 * Maps JDBC/HiveServer2-shaped metadata results (`TABLE_CAT`, `TABLE_SCHEM`, ...) onto the
 * fixed Flight SQL metadata schemas by column name. The engines return metadata in JDBC
 * column order, which differs from Flight SQL (e.g. GetSchemas is `TABLE_SCHEM,
 * TABLE_CATALOG` in JDBC but `catalog_name, db_schema_name` in Flight SQL), so positional
 * copying swaps or misplaces values.
 */
object KyuubiFlightMetadata {

  type MetadataRow = Map[String, AnyRef]

  /** Flight field -> candidate JDBC source columns (first non-null wins). */
  type Sources = Map[String, Seq[String]]

  val CATALOGS_SOURCES: Sources = Map("catalog_name" -> Seq("TABLE_CAT", "TABLE_CATALOG"))

  val SCHEMAS_SOURCES: Sources = Map(
    "catalog_name" -> Seq("TABLE_CATALOG", "TABLE_CAT"),
    "db_schema_name" -> Seq("TABLE_SCHEM"))

  val TABLES_SOURCES: Sources = Map(
    "catalog_name" -> Seq("TABLE_CAT", "TABLE_CATALOG"),
    "db_schema_name" -> Seq("TABLE_SCHEM"),
    "table_name" -> Seq("TABLE_NAME"),
    "table_type" -> Seq("TABLE_TYPE"))

  val TABLE_TYPES_SOURCES: Sources = Map("table_type" -> Seq("TABLE_TYPE"))

  val PRIMARY_KEYS_SOURCES: Sources = Map(
    "catalog_name" -> Seq("TABLE_CAT"),
    "db_schema_name" -> Seq("TABLE_SCHEM"),
    "table_name" -> Seq("TABLE_NAME"),
    "column_name" -> Seq("COLUMN_NAME"),
    "key_name" -> Seq("PK_NAME"),
    // Hive's GetPrimaryKeys names it KEQ_SEQ
    "key_sequence" -> Seq("KEY_SEQ", "KEQ_SEQ"))

  /** Imported keys, exported keys and cross reference share one Flight SQL schema. */
  val FOREIGN_KEYS_SOURCES: Sources = Map(
    "pk_catalog_name" -> Seq("PKTABLE_CAT"),
    "pk_db_schema_name" -> Seq("PKTABLE_SCHEM"),
    "pk_table_name" -> Seq("PKTABLE_NAME"),
    "pk_column_name" -> Seq("PKCOLUMN_NAME"),
    "fk_catalog_name" -> Seq("FKTABLE_CAT"),
    "fk_db_schema_name" -> Seq("FKTABLE_SCHEM"),
    "fk_table_name" -> Seq("FKTABLE_NAME"),
    "fk_column_name" -> Seq("FKCOLUMN_NAME"),
    "key_sequence" -> Seq("KEY_SEQ"),
    "fk_key_name" -> Seq("FK_NAME"),
    "pk_key_name" -> Seq("PK_NAME"),
    "update_rule" -> Seq("UPDATE_RULE"),
    "delete_rule" -> Seq("DELETE_RULE"))

  /**
   * Values for non-nullable integer fields without a source value: Flight SQL key rules use
   * the JDBC codes, so a missing rule is reported as NO ACTION.
   */
  val INT_DEFAULTS: Map[String, Long] = Map(
    "update_rule" -> java.sql.DatabaseMetaData.importedKeyNoAction.toLong,
    "delete_rule" -> java.sql.DatabaseMetaData.importedKeyNoAction.toLong)

  /** Flight GetTables `table_schema` column. */
  val TABLE_SCHEMA_FIELD = "table_schema"

  /** (catalog, schema, table) with null catalog/schema normalized to "". */
  type TableKey = (String, String, String)

  /** Rows of `root` as `lower-case column name -> value` (VarChar values as String). */
  def readRows(root: VectorSchemaRoot): Seq[MetadataRow] = {
    val vectors = root.getFieldVectors.asScala.toSeq
    val names = vectors.map(_.getName.toLowerCase(Locale.ROOT))
    (0 until root.getRowCount).map { i =>
      names.zip(vectors).map { case (name, vector) =>
        name -> normalize(if (vector.isNull(i)) null else vector.getObject(i))
      }.toMap
    }
  }

  private def normalize(value: AnyRef): AnyRef = value match {
    case t: Text => t.toString
    case other => other
  }

  /** Case-insensitive lookup of the first non-null source column. */
  def lookup(row: MetadataRow, candidates: Seq[String]): Option[AnyRef] =
    candidates.iterator
      .map(c => row.get(c.toLowerCase(Locale.ROOT)).orNull)
      .collectFirst { case v if v != null => v }

  def string(row: MetadataRow, column: String): String =
    lookup(row, Seq(column)).map(_.toString).orNull

  def int(row: MetadataRow, column: String): Option[Int] =
    lookup(row, Seq(column)).flatMap {
      case n: Number => Some(n.intValue())
      case s: String if s.trim.nonEmpty => scala.util.Try(s.trim.toInt).toOption
      case _ => None
    }

  private def toLong(value: AnyRef): Option[Long] = value match {
    case n: Number => Some(n.longValue())
    case s: String if s.trim.nonEmpty => scala.util.Try(s.trim.toLong).toOption
    case _ => None
  }

  def tableKey(catalog: String, schema: String, table: String): TableKey =
    (Option(catalog).getOrElse(""), Option(schema).getOrElse(""), table)

  private def rowTableKey(row: MetadataRow): TableKey =
    tableKey(string(row, "TABLE_CAT"), string(row, "TABLE_SCHEM"), string(row, "TABLE_NAME"))

  /**
   * Fill `root` (a Flight SQL metadata schema) from JDBC-shaped rows. Non-nullable Utf8 fields
   * without a value get "", non-nullable integer fields [[INT_DEFAULTS]] or 0 (Flight requires
   * them); `table_schema` comes from `tableSchemas`
   * (an empty schema when the table has no column information).
   */
  def fill(
      root: VectorSchemaRoot,
      rows: Seq[MetadataRow],
      sources: Sources,
      tableSchemas: Map[TableKey, Array[Byte]] = Map.empty): Unit = {
    root.allocateNew()
    root.getSchema.getFields.asScala.foreach { field =>
      val vector: FieldVector = root.getVector(field.getName)
      rows.zipWithIndex.foreach { case (row, i) =>
        vector match {
          case binary: VarBinaryVector if field.getName == TABLE_SCHEMA_FIELD =>
            val key = rowTableKey(row)
            binary.setSafe(i, tableSchemas.getOrElse(key, EMPTY_SCHEMA))
          case varchar: VarCharVector =>
            lookup(row, sources.getOrElse(field.getName, Seq.empty)).map(_.toString) match {
              case Some(v) => varchar.setSafe(i, v.getBytes(StandardCharsets.UTF_8))
              case None if field.isNullable => varchar.setNull(i)
              case None => varchar.setSafe(i, Array.emptyByteArray)
            }
          case integer: BaseIntVector =>
            val name = field.getName
            lookup(row, sources.getOrElse(name, Seq.empty)).flatMap(toLong) match {
              case Some(v) => integer.setWithPossibleTruncate(i, v)
              case None if field.isNullable => integer.setNull(i)
              case None => integer.setWithPossibleTruncate(i, INT_DEFAULTS.getOrElse(name, 0L))
            }
          case other => other.setNull(i)
        }
      }
      vector.setValueCount(rows.size)
    }
    root.setRowCount(rows.size)
  }

  private lazy val EMPTY_SCHEMA: Array[Byte] =
    new Schema(Collections.emptyList[Field]()).serializeAsMessage()

  /**
   * IPC-serialized Arrow schema per table, built from JDBC GetColumns rows
   * (`COLUMN_NAME`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS`, `NULLABLE`,
   * `ORDINAL_POSITION`).
   */
  def tableSchemas(columnRows: Seq[MetadataRow]): Map[TableKey, Array[Byte]] =
    columnRows
      .groupBy(rowTableKey)
      .map { case (key, columns) =>
        val fields = columns
          .sortBy(c => int(c, "ORDINAL_POSITION").getOrElse(Int.MaxValue))
          .map { c =>
            val nullable = !int(c, "NULLABLE").contains(java.sql.DatabaseMetaData.columnNoNulls)
            val arrowType = toArrowType(
              int(c, "DATA_TYPE").getOrElse(Types.VARCHAR),
              int(c, "COLUMN_SIZE"),
              int(c, "DECIMAL_DIGITS"))
            new Field(
              string(c, "COLUMN_NAME"),
              new FieldType(nullable, arrowType, null),
              Collections.emptyList[Field]())
          }
        key -> new Schema(fields.asJava).serializeAsMessage()
      }

  /** JDBC type code -> Arrow type (complex/unknown types are exposed as Utf8). */
  def toArrowType(jdbcType: Int, columnSize: Option[Int], decimalDigits: Option[Int]): ArrowType =
    jdbcType match {
      case Types.BOOLEAN | Types.BIT => ArrowType.Bool.INSTANCE
      case Types.TINYINT => new ArrowType.Int(8, true)
      case Types.SMALLINT => new ArrowType.Int(16, true)
      case Types.INTEGER => new ArrowType.Int(32, true)
      case Types.BIGINT => new ArrowType.Int(64, true)
      case Types.FLOAT | Types.REAL => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case Types.DOUBLE => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case Types.DECIMAL | Types.NUMERIC =>
        val precision = columnSize.filter(p => p > 0 && p <= 38).getOrElse(38)
        val scale = decimalDigits.filter(s => s >= 0 && s <= precision).getOrElse(0)
        new ArrowType.Decimal(precision, scale, 128)
      case Types.DATE => new ArrowType.Date(DateUnit.DAY)
      case Types.TIMESTAMP => new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
      case Types.TIMESTAMP_WITH_TIMEZONE => new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => ArrowType.Binary.INSTANCE
      case _ => ArrowType.Utf8.INSTANCE
    }
}
