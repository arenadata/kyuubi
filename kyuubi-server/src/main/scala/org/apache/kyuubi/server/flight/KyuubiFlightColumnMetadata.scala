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

import java.util.Locale

import org.apache.arrow.flight.sql.FlightSqlColumnMetadata

import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId

/**
 * Flight SQL column metadata (ARROW:FLIGHT:SQL:*) for result and table columns. ODBC and
 * JDBC clients read the type name, precision, scale, case sensitivity and searchability
 * from it; without it the Arrow Flight SQL ODBC driver reports generic types, 1024
 * characters for strings and "not searchable".
 */
private[flight] object KyuubiFlightColumnMetadata {

  /** Base type name of an HS2 type id: INT, DECIMAL, VARCHAR, ARRAY, ... */
  def baseName(id: TTypeId): String = id.name().stripSuffix("_TYPE")

  /** Base type name of an engine type name: decimal(10,2) -> DECIMAL, ARRAY<INT> -> ARRAY. */
  def baseName(typeName: String): String =
    Option(typeName).map(_.trim.toUpperCase(Locale.ROOT)).filter(_.nonEmpty)
      .map(_.takeWhile(c => c != '(' && c != '<' && !Character.isWhitespace(c)))
      .getOrElse("STRING")

  private val StringTypes = Set("STRING", "VARCHAR", "CHAR")
  private val ComplexTypes = Set("ARRAY", "MAP", "STRUCT", "UNION")

  /**
   * Length reported for STRING, BINARY and complex types (no declared length):
   * Int.MaxValue, as HS2 GetColumns reports for STRING, so JDBC clients show STRING, not
   * STRING(n). The ODBC driver's own default is 1024; a DSN stringColumnLength overrides
   * it on the client.
   */
  val StringColumnLength: Int = Int.MaxValue

  /**
   * Metadata for a column of type `name` (a base name). `precision` / `scale` / `length`
   * may be None; [[StringColumnLength]] is reported for STRING, BINARY and complex types.
   * `schema` / `table` only for table columns.
   */
  def apply(
      name: String,
      precision: Option[Int],
      scale: Option[Int],
      length: Option[Int],
      resultColumn: Boolean,
      schema: Option[String] = None,
      table: Option[String] = None): java.util.Map[String, String] = {
    val b = new FlightSqlColumnMetadata.Builder()
      .typeName(name)
      .isAutoIncrement(false)
      .isReadOnly(resultColumn)
      .isCaseSensitive(StringTypes.contains(name))
      .isSearchable(!ComplexTypes.contains(name))
    name match {
      case "DECIMAL" =>
        precision.foreach(b.precision)
        scale.foreach(b.scale)
      case "VARCHAR" | "CHAR" => b.precision(length.filter(_ > 0).getOrElse(StringColumnLength))
      case "STRING" | "BINARY" => b.precision(StringColumnLength)
      case n if ComplexTypes.contains(n) => b.precision(StringColumnLength)
      // values are sent with microseconds
      case "TIMESTAMP" => b.scale(6)
      case _ =>
    }
    schema.foreach(b.schemaName)
    table.foreach(b.tableName)
    b.build().getMetadataMap
  }
}
