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

import java.sql.{Connection, ResultSet, Types}
import java.util.Locale
import java.util.regex.Pattern

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.jdbc.schema.{Column, Row}
import org.apache.kyuubi.util.JdbcUtils

/**
 * JDBC `DatabaseMetaData`-shaped metadata for Impala, built from `SHOW DATABASES`,
 * `SHOW TABLES`, `SHOW VIEWS` and `DESCRIBE` (Impala has no `information_schema`).
 *
 * @param run executes a statement and returns its rows as `column label (lower case) -> value`
 */
class ImpalaMetadata(run: String => Seq[Map[String, String]]) extends Logging {

  import ImpalaMetadata._

  /** Impala has no catalogs. */
  def catalogs(): Seq[Row] = Seq.empty

  def schemas(schemaPattern: String): Seq[Row] =
    databases(schemaPattern).map(db => Row(List(db, null)))

  def tables(
      schemaPattern: String,
      tableNamePattern: String,
      tableTypes: Seq[String]): Seq[Row] = {
    val wantedTypes = tableTypes.map(_.trim.toUpperCase(Locale.ROOT)).filter(_.nonEmpty).toSet
    val tableMatcher = matcher(tableNamePattern)
    for {
      db <- databases(schemaPattern)
      (table, tableType) <- tablesWithTypes(db)
      if tableMatcher(table)
      if wantedTypes.isEmpty || wantedTypes.contains(tableType)
    } yield Row(List(null, db, table, tableType, null, null, null, null, null, null))
  }

  def columns(
      schemaPattern: String,
      tableNamePattern: String,
      columnNamePattern: String): Seq[Row] = {
    val tableMatcher = matcher(tableNamePattern)
    val columnMatcher = matcher(columnNamePattern)
    for {
      db <- databases(schemaPattern)
      table <- showNames(s"SHOW TABLES IN ${quote(db)}") if tableMatcher(table)
      (column, position) <- describe(db, table).zipWithIndex
      if columnMatcher(column.name)
    } yield columnRow(db, table, column, position + 1)
  }

  private def databases(schemaPattern: String): Seq[String] = {
    val schemaMatcher = matcher(schemaPattern)
    showNames("SHOW DATABASES").filter(schemaMatcher)
  }

  private def tablesWithTypes(db: String): Seq[(String, String)] = {
    val views =
      try showNames(s"SHOW VIEWS IN ${quote(db)}").toSet
      catch {
        // SHOW VIEWS exists since Impala 4.1; older versions report everything as TABLE
        case NonFatal(e) =>
          debug(s"SHOW VIEWS failed for $db, reporting all objects as TABLE: ${e.getMessage}")
          Set.empty[String]
      }
    showNames(s"SHOW TABLES IN ${quote(db)}").map { table =>
      table -> (if (views.contains(table)) VIEW else TABLE)
    }
  }

  private def showNames(sql: String): Seq[String] =
    run(sql).flatMap(_.get("name")).filter(_ != null)

  private def describe(db: String, table: String): Seq[ImpalaColumn] =
    run(s"DESCRIBE ${quote(db)}.${quote(table)}").flatMap { row =>
      row.get("name").filter(n => n != null && n.nonEmpty).map { name =>
        ImpalaColumn(
          name,
          Option(row.getOrElse("type", null)).getOrElse("string"),
          row.get("comment").filter(c => c != null && c.nonEmpty),
          // Iceberg/Kudu tables report nullability; others are always nullable
          row.get("nullable").filter(_ != null).forall(!_.trim.equalsIgnoreCase("false")))
      }
    }

  private def columnRow(db: String, table: String, column: ImpalaColumn, position: Int): Row = {
    val t = toJdbcType(column.dataType)
    Row(List(
      null, // TABLE_CAT
      db, // TABLE_SCHEM
      table, // TABLE_NAME
      column.name, // COLUMN_NAME
      Int.box(t.sqlType), // DATA_TYPE
      t.typeName, // TYPE_NAME
      t.columnSize.map(Int.box).orNull, // COLUMN_SIZE
      null, // BUFFER_LENGTH
      t.decimalDigits.map(Int.box).orNull, // DECIMAL_DIGITS
      t.radix.map(Int.box).orNull, // NUM_PREC_RADIX
      Int.box(if (column.nullable) NULLABLE else NO_NULLS), // NULLABLE
      column.comment.orNull, // REMARKS
      null, // COLUMN_DEF
      null, // SQL_DATA_TYPE
      null, // SQL_DATETIME_SUB
      null, // CHAR_OCTET_LENGTH
      Int.box(position), // ORDINAL_POSITION
      if (column.nullable) "YES" else "NO", // IS_NULLABLE
      null, // SCOPE_CATALOG
      null, // SCOPE_SCHEMA
      null, // SCOPE_TABLE
      null, // SOURCE_DATA_TYPE
      "NO" // IS_AUTO_INCREMENT
    ))
  }
}

object ImpalaMetadata {

  val TABLE = "TABLE"
  val VIEW = "VIEW"

  private val NO_NULLS = java.sql.DatabaseMetaData.columnNoNulls
  private val NULLABLE = java.sql.DatabaseMetaData.columnNullable

  private def string(name: String): Column =
    Column(name, "STRING", Types.VARCHAR, Int.MaxValue, 0, name, Int.MaxValue)

  private def int(name: String): Column =
    Column(name, "INT", Types.INTEGER, 10, 0, name, 11)

  private def smallint(name: String): Column =
    Column(name, "SMALLINT", Types.SMALLINT, 5, 0, name, 6)

  val CATALOGS_COLUMNS: List[Column] = List(string("TABLE_CAT"))

  val SCHEMAS_COLUMNS: List[Column] = List(string("TABLE_SCHEM"), string("TABLE_CATALOG"))

  val TABLE_TYPES_COLUMNS: List[Column] = List(string("TABLE_TYPE"))

  val TABLE_TYPES_ROWS: Seq[Row] = Seq(Row(List(TABLE)), Row(List(VIEW)))

  val TABLES_COLUMNS: List[Column] = List(
    string("TABLE_CAT"),
    string("TABLE_SCHEM"),
    string("TABLE_NAME"),
    string("TABLE_TYPE"),
    string("REMARKS"),
    string("TYPE_CAT"),
    string("TYPE_SCHEM"),
    string("TYPE_NAME"),
    string("SELF_REFERENCING_COL_NAME"),
    string("REF_GENERATION"))

  val COLUMNS_COLUMNS: List[Column] = List(
    string("TABLE_CAT"),
    string("TABLE_SCHEM"),
    string("TABLE_NAME"),
    string("COLUMN_NAME"),
    int("DATA_TYPE"),
    string("TYPE_NAME"),
    int("COLUMN_SIZE"),
    int("BUFFER_LENGTH"),
    int("DECIMAL_DIGITS"),
    int("NUM_PREC_RADIX"),
    int("NULLABLE"),
    string("REMARKS"),
    string("COLUMN_DEF"),
    int("SQL_DATA_TYPE"),
    int("SQL_DATETIME_SUB"),
    int("CHAR_OCTET_LENGTH"),
    int("ORDINAL_POSITION"),
    string("IS_NULLABLE"),
    string("SCOPE_CATALOG"),
    string("SCOPE_SCHEMA"),
    string("SCOPE_TABLE"),
    smallint("SOURCE_DATA_TYPE"),
    string("IS_AUTO_INCREMENT"))

  val PRIMARY_KEYS_COLUMNS: List[Column] = List(
    string("TABLE_CAT"),
    string("TABLE_SCHEM"),
    string("TABLE_NAME"),
    string("COLUMN_NAME"),
    int("KEY_SEQ"),
    string("PK_NAME"))

  val CROSS_REFERENCE_COLUMNS: List[Column] = List(
    string("PKTABLE_CAT"),
    string("PKTABLE_SCHEM"),
    string("PKTABLE_NAME"),
    string("PKCOLUMN_NAME"),
    string("FKTABLE_CAT"),
    string("FKTABLE_SCHEM"),
    string("FKTABLE_NAME"),
    string("FKCOLUMN_NAME"),
    int("KEY_SEQ"),
    smallint("UPDATE_RULE"),
    smallint("DELETE_RULE"),
    string("FK_NAME"),
    string("PK_NAME"),
    smallint("DEFERRABILITY"))

  /**
   * Informational primary keys (`PRIMARY KEY ... DISABLE NOVALIDATE RELY`, Kudu keys) through
   * Impala's HiveServer2 GetPrimaryKeys. Impala has no catalogs.
   */
  def primaryKeys(connection: Connection, schema: String, table: String): Seq[Row] =
    toKeyRows(
      fetch(connection.getMetaData.getPrimaryKeys(null, blankToNull(schema), table)),
      PRIMARY_KEYS_COLUMNS)

  /**
   * Foreign keys through Impala's HiveServer2 GetCrossReference: either side may be left
   * out (null) to list the keys a table exports (parent given) or imports (foreign given).
   */
  def crossReference(
      connection: Connection,
      parentSchema: String,
      parentTable: String,
      foreignSchema: String,
      foreignTable: String): Seq[Row] =
    toKeyRows(
      fetch(connection.getMetaData.getCrossReference(
        null,
        blankToNull(parentSchema),
        blankToNull(parentTable),
        null,
        blankToNull(foreignSchema),
        blankToNull(foreignTable))),
      CROSS_REFERENCE_COLUMNS)

  private def blankToNull(value: String): String =
    Option(value).map(_.trim).filter(v => v.nonEmpty && v != "%" && v != "*").orNull

  /** All rows of `rs` (closed afterwards), values by position. */
  private def fetch(resultSet: ResultSet): Seq[Seq[AnyRef]] =
    JdbcUtils.withCloseable(resultSet) { rs =>
      val count = rs.getMetaData.getColumnCount
      val rows = ArrayBuffer[Seq[AnyRef]]()
      while (rs.next()) rows += (1 to count).map(i => rs.getObject(i))
      rows.toSeq
    }

  /**
   * Positional JDBC key rows -> rows typed for `columns` (integer columns as Int; Hive's
   * driver names the primary key sequence column `KEQ_SEQ`, so names are not used).
   */
  def toKeyRows(raw: Seq[Seq[AnyRef]], columns: List[Column]): Seq[Row] =
    raw.map { values =>
      Row(columns.zipWithIndex.map { case (column, i) =>
        val value = if (i < values.size) values(i) else null
        column.sqlType match {
          case Types.INTEGER | Types.SMALLINT => toInt(value)
          case _ => Option(value).map(_.toString).orNull
        }
      })
    }

  private def toInt(value: AnyRef): Any = value match {
    case null => null
    case n: Number => Int.box(n.intValue())
    case s => scala.util.Try(Int.box(s.toString.trim.toInt)).toOption.orNull
  }

  case class ImpalaColumn(
      name: String,
      dataType: String,
      comment: Option[String],
      nullable: Boolean)

  case class JdbcType(
      sqlType: Int,
      typeName: String,
      columnSize: Option[Int] = None,
      decimalDigits: Option[Int] = None,
      radix: Option[Int] = None)

  private val SIMPLE_TYPE = """^([a-z_]+)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?$""".r

  /** Impala type string (as printed by DESCRIBE) -> JDBC type information. */
  def toJdbcType(impalaType: String): JdbcType = {
    val normalized = impalaType.trim.replaceAll("\\s+", " ")
    val lower = normalized.toLowerCase(Locale.ROOT)
    if (lower.startsWith("array<")) {
      JdbcType(Types.ARRAY, normalized.toUpperCase(Locale.ROOT))
    } else if (lower.startsWith("map<")) {
      JdbcType(Types.JAVA_OBJECT, normalized.toUpperCase(Locale.ROOT))
    } else if (lower.startsWith("struct<")) {
      JdbcType(Types.STRUCT, normalized.toUpperCase(Locale.ROOT))
    } else {
      lower match {
        case SIMPLE_TYPE(base, p, s) =>
          val precision = Option(p).map(_.toInt)
          val scale = Option(s).map(_.toInt)
          base match {
            case "boolean" => JdbcType(Types.BOOLEAN, "BOOLEAN")
            case "tinyint" => JdbcType(Types.TINYINT, "TINYINT", Some(3), Some(0), Some(10))
            case "smallint" => JdbcType(Types.SMALLINT, "SMALLINT", Some(5), Some(0), Some(10))
            case "int" | "integer" =>
              JdbcType(Types.INTEGER, "INT", Some(10), Some(0), Some(10))
            case "bigint" => JdbcType(Types.BIGINT, "BIGINT", Some(19), Some(0), Some(10))
            case "float" | "real" => JdbcType(Types.FLOAT, "FLOAT", Some(7), None, Some(10))
            case "double" => JdbcType(Types.DOUBLE, "DOUBLE", Some(15), None, Some(10))
            case "decimal" =>
              // Impala DECIMAL defaults to DECIMAL(9,0)
              val pr = precision.getOrElse(9)
              val sc = scale.getOrElse(0)
              JdbcType(Types.DECIMAL, s"DECIMAL($pr,$sc)", Some(pr), Some(sc), Some(10))
            case "string" => JdbcType(Types.VARCHAR, "STRING", Some(Int.MaxValue))
            case "varchar" =>
              JdbcType(
                Types.VARCHAR,
                precision.map(n => s"VARCHAR($n)").getOrElse("VARCHAR"),
                precision.orElse(Some(Int.MaxValue)))
            case "char" =>
              JdbcType(
                Types.CHAR,
                precision.map(n => s"CHAR($n)").getOrElse("CHAR"),
                precision.orElse(Some(1)))
            case "timestamp" => JdbcType(Types.TIMESTAMP, "TIMESTAMP", Some(29), Some(9))
            case "date" => JdbcType(Types.DATE, "DATE", Some(10))
            case "binary" => JdbcType(Types.BINARY, "BINARY", Some(Int.MaxValue))
            case other => JdbcType(Types.OTHER, other.toUpperCase(Locale.ROOT))
          }
        case _ => JdbcType(Types.OTHER, normalized.toUpperCase(Locale.ROOT))
      }
    }
  }

  /**
   * JDBC search pattern matcher. `null`, empty, `%` and `*` match everything; `%`/`*` match
   * any sequence and `_` one character (SQL LIKE plus the `*` Hive clients send); `\` escapes.
   * Impala identifiers are case-insensitive, so matching is too.
   */
  def matcher(pattern: String): String => Boolean = {
    if (pattern == null || pattern.isEmpty || pattern == "%" || pattern == "*") {
      _ => true
    } else {
      val regex = new StringBuilder
      var i = 0
      while (i < pattern.length) {
        pattern.charAt(i) match {
          case '\\' if i + 1 < pattern.length =>
            i += 1
            regex.append(Pattern.quote(pattern.charAt(i).toString))
          case '%' | '*' => regex.append(".*")
          case '_' => regex.append('.')
          case c => regex.append(Pattern.quote(c.toString))
        }
        i += 1
      }
      val compiled = Pattern.compile(regex.toString, Pattern.CASE_INSENSITIVE)
      name => name != null && compiled.matcher(name).matches()
    }
  }

  /** Back-quoted Impala identifier. */
  def quote(identifier: String): String = "`" + identifier.replace("`", "``") + "`"

  /** Runs a statement on `connection`, returning rows as `lower-case column label -> value`. */
  def runner(connection: Connection): String => Seq[Map[String, String]] = sql => {
    JdbcUtils.withCloseable(connection.createStatement()) { statement =>
      JdbcUtils.withCloseable(statement.executeQuery(sql)) { rs =>
        val meta = rs.getMetaData
        val labels = (1 to meta.getColumnCount)
          .map(i => meta.getColumnLabel(i).toLowerCase(Locale.ROOT))
        val rows = ArrayBuffer[Map[String, String]]()
        while (rs.next()) {
          rows += labels.zipWithIndex.map { case (label, i) =>
            label -> rs.getString(i + 1)
          }.toMap
        }
        rows.toSeq
      }
    }
  }
}
