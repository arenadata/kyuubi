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
package org.apache.kyuubi.engine.jdbc.dialect

import java.util

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.jdbc.impala.{ImpalaMetadata, ImpalaSchemaHelper, ImpalaTRowSetGenerator}
import org.apache.kyuubi.engine.jdbc.impala.ImpalaMetadata._
import org.apache.kyuubi.engine.jdbc.operation.JdbcMetadataOperation
import org.apache.kyuubi.engine.jdbc.schema.{Column, JdbcTRowSetGenerator, Row, SchemaHelper}
import org.apache.kyuubi.operation.Operation
import org.apache.kyuubi.session.Session

class ImpalaDialect extends JdbcDialect with JdbcMetadataOperations {

  // JDBC metadata (GetCatalogs/GetSchemas/GetTables/GetTableTypes/GetColumns) is assembled
  // from SHOW/DESCRIBE output by ImpalaMetadata; GetPrimaryKeys/GetCrossReference go to
  // Impala's own HiveServer2 calls; the query-string methods below
  // (getTablesQuery, getColumnsQuery) are kept for compatibility but no longer used by the
  // operation manager.

  private def metadataOperation(
      session: Session,
      columns: List[Column])(rows: ImpalaMetadata => Seq[Row]): Operation =
    new JdbcMetadataOperation(
      session,
      columns,
      connection => rows(new ImpalaMetadata(ImpalaMetadata.runner(connection))))

  override def catalogsOperation(session: Session): Operation =
    metadataOperation(session, CATALOGS_COLUMNS)(_.catalogs())

  override def schemasOperation(
      session: Session,
      catalog: String,
      schemaPattern: String): Operation =
    metadataOperation(session, SCHEMAS_COLUMNS)(_.schemas(schemaPattern))

  override def tablesOperation(
      session: Session,
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      tableTypes: util.List[String]): Operation = {
    val types = Option(tableTypes).map(_.asScala.toSeq).getOrElse(Seq.empty)
    metadataOperation(session, TABLES_COLUMNS)(_.tables(schemaPattern, tableNamePattern, types))
  }

  override def columnsOperation(
      session: Session,
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      columnNamePattern: String): Operation =
    metadataOperation(session, COLUMNS_COLUMNS)(
      _.columns(schemaPattern, tableNamePattern, columnNamePattern))

  override def primaryKeysOperation(
      session: Session,
      catalog: String,
      schema: String,
      table: String): Operation =
    new JdbcMetadataOperation(
      session,
      PRIMARY_KEYS_COLUMNS,
      connection => ImpalaMetadata.primaryKeys(connection, schema, table))

  override def crossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation =
    new JdbcMetadataOperation(
      session,
      CROSS_REFERENCE_COLUMNS,
      connection =>
        ImpalaMetadata.crossReference(
          connection,
          primarySchema,
          primaryTable,
          foreignSchema,
          foreignTable))

  override def getTableTypesOperation(session: Session): Operation =
    new JdbcMetadataOperation(session, TABLE_TYPES_COLUMNS, _ => TABLE_TYPES_ROWS)

  override def getTablesQuery(
      catalog: String,
      schema: String,
      tableName: String,
      tableTypes: util.List[String]): String = {
    if (isPattern(schema)) {
      throw KyuubiSQLException.featureNotSupported("Pattern-like schema names not supported")
    }

    val query = new StringBuilder("show tables ")

    if (StringUtils.isNotEmpty(schema) && !isWildcardSetByKyuubi(schema)) {
      query.append(s"in $schema ")
    }

    if (StringUtils.isNotEmpty(tableName)) {
      query.append(s"like '${toImpalaRegex(tableName)}'")
    }

    query.toString()
  }

  override def getColumnsQuery(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): String = {
    if (StringUtils.isEmpty(tableName)) {
      throw KyuubiSQLException("Table name should not be empty")
    }

    if (isPattern(schemaName)) {
      throw KyuubiSQLException.featureNotSupported("Pattern-like schema names not supported")
    }

    if (isPattern(tableName)) {
      throw KyuubiSQLException.featureNotSupported("Pattern-like table names not supported")
    }

    val query = new StringBuilder("show column stats ")

    if (StringUtils.isNotEmpty(schemaName) && !isWildcardSetByKyuubi(schemaName)) {
      query.append(s"$schemaName.")
    }

    query.append(tableName)
    query.toString()
  }

  override def getTRowSetGenerator(): JdbcTRowSetGenerator = new ImpalaTRowSetGenerator

  override def getSchemaHelper(): SchemaHelper = new ImpalaSchemaHelper

  override def name(): String = "impala"

  private def isPattern(value: String): Boolean = {
    value != null && !isWildcardSetByKyuubi(value) && value.contains("*")
  }

  private def isWildcardSetByKyuubi(pattern: String): Boolean = pattern == "%"

  private def toImpalaRegex(pattern: String): String = {
    pattern.replace("%", "*")
  }
}
