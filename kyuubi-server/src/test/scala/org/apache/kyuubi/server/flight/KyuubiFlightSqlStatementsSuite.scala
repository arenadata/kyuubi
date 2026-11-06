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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.flight.KyuubiFlightSqlStatements.{firstKeyword, isQuery}

class KyuubiFlightSqlStatementsSuite extends KyuubiFunSuite {

  test("queries are recognised by their first keyword") {
    Seq(
      "SELECT 1",
      "select * from t",
      "  \n\tSelect 1",
      "WITH a AS (SELECT 1) SELECT * FROM a",
      "VALUES (1), (2)",
      "SHOW DATABASES",
      "show tables in vdmitriev",
      "DESCRIBE t",
      "desc t",
      "EXPLAIN SELECT 1",
      "TABLE t",
      "(SELECT 1) UNION ALL (SELECT 2)").foreach { sql =>
      assert(isQuery(sql), sql)
    }
  }

  test("DDL, DML and commands are not queries") {
    Seq(
      "CREATE TABLE t (a INT)",
      "insert into t values (1)",
      "INSERT OVERWRITE TABLE t SELECT 1",
      "FROM src INSERT INTO t SELECT *",
      "DROP TABLE t",
      "ALTER TABLE t ADD COLUMNS (b INT)",
      "SET kyuubi.operation.language=SQL",
      "USE vdmitriev",
      "UPDATE t SET a = 1",
      "DELETE FROM t",
      "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE",
      "TRUNCATE TABLE t",
      "",
      "   ",
      null).foreach { sql =>
      assert(!isQuery(sql), String.valueOf(sql))
    }
  }

  test("leading comments are skipped") {
    assert(firstKeyword("-- comment\nSELECT 1").contains("select"))
    assert(firstKeyword("/* multi\n line */ insert into t values (1)").contains("insert"))
    assert(firstKeyword("-- a\n/* b */\n  -- c\n show tables").contains("show"))
    assert(isQuery("/* hint */ SELECT 1"))
    assert(!isQuery("-- only a comment"))
    assert(!isQuery("/* unterminated"))
  }
}
