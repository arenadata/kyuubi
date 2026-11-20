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

import org.apache.kyuubi.operation.Operation
import org.apache.kyuubi.session.Session

/**
 * Optional dialect capability: build JDBC metadata results as operations instead of single
 * SQL queries. When a dialect mixes this in, the operation manager uses these methods for
 * GetCatalogs / GetSchemas / GetTables / GetColumns instead of the query-string methods of
 * [[JdbcDialect]].
 */
trait JdbcMetadataOperations {

  def catalogsOperation(session: Session): Operation

  def schemasOperation(session: Session, catalog: String, schemaPattern: String): Operation

  def tablesOperation(
      session: Session,
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      tableTypes: util.List[String]): Operation

  def columnsOperation(
      session: Session,
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      columnNamePattern: String): Operation

  def primaryKeysOperation(
      session: Session,
      catalog: String,
      schema: String,
      table: String): Operation

  def crossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation
}
