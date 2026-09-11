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

package org.apache.kyuubi.gateway.jdbc

import java.util

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect
import org.apache.kyuubi.engine.jdbc.schema.DefaultJdbcTRowSetGenerator
import org.apache.kyuubi.engine.jdbc.schema.JdbcTRowSetGenerator
import org.apache.kyuubi.engine.jdbc.schema.SchemaHelper
import org.apache.kyuubi.session.Session

/**
 * A Spark Thrift Server, reached as an ordinary JDBC backend.
 *
 * Kyuubi's own Spark support launches an engine and finds it through
 * ZooKeeper, which this gateway removed along with the rest of the engine
 * machinery. A Thrift Server is the shape that fits what is left: it is already
 * running, it speaks HS2, and the Hive driver reaches it - so it is a cluster
 * like any other, discovered from a Service and routed to by identity.
 *
 * What that buys is routing and impersonation. Admission, sizing and scaling
 * stay with Trino: Spark's own scheduler decides what runs, and a second
 * opinion from outside would fight it rather than help.
 *
 * No connection provider accompanies this. Providers are keyed on the driver
 * rather than the engine, and the one the JDBC engine ships for Impala already
 * handles `org.apache.kyuubi.jdbc.KyuubiHiveDriver` - a second one claiming the
 * same driver would make the choice between them ambiguous.
 */
class SparkDialect extends JdbcDialect {

  import SparkDialect._

  override def name(): String = "spark"

  override def getSchemasOperation(catalog: String, schema: String): String = {
    val query = new StringBuilder("SHOW DATABASES")
    if (StringUtils.isNotEmpty(schema) && !isWildcard(schema)) {
      query.append(s" LIKE '${toSparkPattern(schema)}'")
    }
    query.toString()
  }

  override def getTablesQuery(
      catalog: String,
      schema: String,
      tableName: String,
      tableTypes: util.List[String]): String = {
    val query = new StringBuilder("SHOW TABLES")
    if (StringUtils.isNotEmpty(schema) && !isWildcard(schema)) {
      if (isPattern(schema)) {
        // SHOW TABLES takes one database, not a pattern of them. Silently
        // listing the current database instead would answer a different
        // question than the one asked.
        throw KyuubiSQLException.featureNotSupported(
          "Spark cannot list tables across a pattern of databases")
      }
      query.append(s" IN $schema")
    }
    if (StringUtils.isNotEmpty(tableName)) {
      query.append(s" LIKE '${toSparkPattern(tableName)}'")
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
    if (isPattern(schemaName) || isPattern(tableName)) {
      throw KyuubiSQLException.featureNotSupported(
        "Spark describes one table at a time, so patterns are not supported here")
    }

    val query = new StringBuilder("DESCRIBE TABLE ")
    if (StringUtils.isNotEmpty(schemaName) && !isWildcard(schemaName)) {
      query.append(s"$schemaName.")
    }
    query.append(tableName)
    query.toString()
  }

  // Spark reports its types over HS2 as the defaults expect, so neither of
  // these needs the corrections Impala's do.
  override def getTRowSetGenerator(): JdbcTRowSetGenerator = new DefaultJdbcTRowSetGenerator

  override def getSchemaHelper(): SchemaHelper = new SparkSchemaHelper
}

/** Spark needs no type corrections; the class exists because SchemaHelper is abstract. */
class SparkSchemaHelper extends SchemaHelper

object SparkDialect {

  /** What Kyuubi sends when the client asked for everything. */
  private def isWildcard(pattern: String): Boolean = pattern == "%"

  private def isPattern(value: String): Boolean =
    value != null && !isWildcard(value) && (value.contains("%") || value.contains("*"))

  /**
   * Spark's `LIKE` here is not SQL's.
   *
   * `SHOW TABLES LIKE` matches on `*` and `|`, so a `%` arriving from a JDBC
   * client would be taken literally and match a table nobody has.
   */
  private[jdbc] def toSparkPattern(pattern: String): String = pattern.replace("%", "*")
}
