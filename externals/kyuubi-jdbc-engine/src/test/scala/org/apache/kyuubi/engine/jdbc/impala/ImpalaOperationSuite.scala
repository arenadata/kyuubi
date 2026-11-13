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

import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.operation.HiveJDBCTestHelper

abstract class ImpalaOperationSuite extends WithImpalaEngine with HiveJDBCTestHelper {

  // Metadata follows java.sql.DatabaseMetaData: columns are read by their JDBC names, and a null
  // schema pattern means every schema (not only the current database).

  /** Drops the test objects even when an assertion failed, so later tests start clean. */
  private def cleanUp(statement: java.sql.Statement, sqls: String*): Unit =
    sqls.foreach { sql =>
      try statement.execute(sql)
      catch { case _: java.sql.SQLException => }
    }

  test("impala - get tables") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      try {
        statement.execute("create table test1(id bigint)")
        statement.execute("create table test2(id bigint)")
        statement.execute("create database db1")
        statement.execute("create table db1.test3(id bigint)")

        def tableNames(schema: String, table: String): Seq[String] = {
          val tables = meta.getTables(null, schema, table, null)
          val names = ArrayBuffer[String]()
          while (tables.next()) {
            names += tables.getString("TABLE_NAME")
          }
          names.toSeq
        }

        assert(tableNames(null, "test1") === Seq("test1"))
        assert(tableNames(null, "test2") === Seq("test2"))
        // Impala's `*` and JDBC's `%` are both accepted as wildcards
        assert(tableNames("default", "test*") === Seq("test1", "test2"))
        assert(tableNames("default", "test%") === Seq("test1", "test2"))
        assert(tableNames("db1", "test*") === Seq("test3"))
        assert(tableNames(null, "test*").sorted === Seq("test1", "test2", "test3"))
      } finally {
        cleanUp(
          statement,
          "drop table if exists test1",
          "drop table if exists test2",
          "drop table if exists db1.test3",
          "drop database if exists db1")
      }
    }
  }

  test("impala - get columns") {
    case class Column(name: String, columnType: String)

    def buildColumn(resultSet: ResultSet): Column = {
      val columnName = resultSet.getString("COLUMN_NAME")
      val columnType = resultSet.getString("TYPE_NAME")
      Column(columnName, columnType)
    }

    withJdbcStatement() { statement =>
      val metadata = statement.getConnection.getMetaData
      try {
        statement.execute("create table if not exists test1" +
          "(id bigint, str1 string, str2 string, age int)")

        statement.execute("create database if not exists db1")
        statement.execute("create table if not exists db1.test2" +
          "(id bigint, str1 string)")

        val resultBuffer = ArrayBuffer[Column]()
        val resultSet1 = metadata.getColumns(null, null, "test1", null)
        while (resultSet1.next()) {
          resultBuffer += buildColumn(resultSet1)
        }

        assert(resultBuffer.contains(Column("id", "BIGINT")))
        assert(resultBuffer.contains(Column("str1", "STRING")))
        assert(resultBuffer.contains(Column("str2", "STRING")))
        assert(resultBuffer.contains(Column("age", "INT")))

        resultBuffer.clear()

        val resultSet2 = metadata.getColumns(null, "db1", "test2", null)
        while (resultSet2.next()) {
          resultBuffer += buildColumn(resultSet2)
        }
        assert(resultBuffer.contains(Column("id", "BIGINT")))
        assert(resultBuffer.contains(Column("str1", "STRING")))
      } finally {
        cleanUp(
          statement,
          "drop table if exists test1",
          "drop table if exists db1.test2",
          "drop database if exists db1")
      }
    }
  }
}
