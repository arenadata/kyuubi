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

import java.util.Collections

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect
import org.apache.kyuubi.util.reflect.ReflectUtils.loadFromServiceLoader

class SparkDialectSuite extends KyuubiFunSuite {

  private val dialect = new SparkDialect
  private val anyType = Collections.emptyList[String]()

  test("the dialect is found by the service loader, so the engine becomes available") {
    // The engine list the gateway accepts is read from the dialects, so this is
    // what makes `kyuubi.gateway.engine=spark` a valid configuration at all.
    val names = loadFromServiceLoader[JdbcDialect]().map(_.name()).toSet
    assert(names.contains("spark"))
    assert(names.contains("impala"), "and the engine's own dialects are still there")
  }

  test("listing tables in a database") {
    assert(dialect.getTablesQuery("", "sales", "", anyType) === "SHOW TABLES IN sales")
    assert(dialect.getTablesQuery("", "%", "", anyType) === "SHOW TABLES")
  }

  test("a name pattern is translated, because Spark's LIKE is not SQL's") {
    // SHOW TABLES LIKE matches on `*`; a `%` arriving from a JDBC client would
    // be taken literally and match a table nobody has.
    assert(
      dialect.getTablesQuery("", "sales", "ord%", anyType) === "SHOW TABLES IN sales LIKE 'ord*'")
    assert(dialect.getTablesQuery("", "%", "%", anyType) === "SHOW TABLES LIKE '*'")
  }

  test("a pattern of databases is refused rather than answered differently") {
    val e = intercept[KyuubiSQLException](dialect.getTablesQuery("", "sa%", "", anyType))
    assert(e.getMessage.contains("pattern of databases"))
  }

  test("describing one table") {
    assert(dialect.getColumnsQuery(
      null,
      "",
      "sales",
      "orders",
      "") === "DESCRIBE TABLE sales.orders")
    assert(dialect.getColumnsQuery(null, "", "%", "orders", "") === "DESCRIBE TABLE orders")
  }

  test("describing something that is not one table is refused") {
    intercept[KyuubiSQLException](dialect.getColumnsQuery(null, "", "sales", "", ""))
    intercept[KyuubiSQLException](dialect.getColumnsQuery(null, "", "sales", "ord%", ""))
  }

  test("listing databases") {
    assert(dialect.getSchemasOperation("", "%") === "SHOW DATABASES")
    assert(dialect.getSchemasOperation("", "sa%") === "SHOW DATABASES LIKE 'sa*'")
  }

  test("Spark needs no type corrections, unlike Impala") {
    // Impala reports float as double and has a helper that says so; Spark
    // reports what the defaults expect, so this is a plain one.
    assert(dialect.getSchemaHelper().isInstanceOf[SparkSchemaHelper])
    assert(dialect.getTRowSetGenerator() != null)
  }
}
