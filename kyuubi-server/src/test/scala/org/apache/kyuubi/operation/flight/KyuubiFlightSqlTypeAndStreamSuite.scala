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

package org.apache.kyuubi.operation.flight

import org.apache.kyuubi.{FlightSqlTestHelper, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS

class KyuubiFlightSqlTypeAndStreamSuite extends WithFlightSqlServer with FlightSqlTestHelper {

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS, 10)

  test("supported primitive types") {
    withFlightSqlClient() { (_, sqlClient) =>
      val sql =
        """
          |SELECT
          |  CAST(NULL AS INT) AS null_col,
          |  true AS bool_col,
          |  CAST(1 AS TINYINT) AS tiny_col,
          |  CAST(2 AS SMALLINT) AS small_col,
          |  CAST(3 AS INT) AS int_col,
          |  CAST(4 AS BIGINT) AS big_col,
          |  CAST(1.5 AS FLOAT) AS float_col,
          |  CAST(2.5 AS DOUBLE) AS double_col,
          |  'text' AS string_col,
          |  CAST('ABCD' AS BINARY) AS binary_col,
          |  CAST(12.34 AS DECIMAL(10,2)) AS decimal_col,
          |  CAST('2024-01-02' AS DATE) AS date_col,
          |  CAST('2024-01-02 03:04:05' AS TIMESTAMP) AS ts_col
          |""".stripMargin
      val rows = executeAndCollect(sqlClient, sql)
      assert(rows.size === 1)
      val row = rows.head
      assert(row.head === null)
      assert(row(1) === java.lang.Boolean.TRUE)
      assert(row(2).toString === "1")
      assert(row(3).toString === "2")
      assert(row(4).toString === "3")
      assert(row(5).toString === "4")
      assert(row(8).toString === "text")
      assert(row(10) != null)
      assert(row(11) != null)
      assert(row(12) != null)
    }
  }

  test("complex type fails deterministically") {
    withFlightSqlClient() { (_, sqlClient) =>
      intercept[Exception] {
        executeAndCollect(sqlClient, "SELECT array(1, 2, 3) AS arr_col")
      }
    }
  }

  test("paging streams multiple batches for large result") {
    withFlightSqlClient() { (_, sqlClient) =>
      val pages = executeAndCollectPaged(sqlClient, "SELECT id FROM range(55)")
      assert(pages.rows.size === 55)
      assert(pages.rows.map(_.head.toString.toLong).sorted === (0L until 55L))
      assertFlightBatchesBoundedByPageSize(
        pages.batchRowCounts,
        expectedTotalRows = 55,
        pageSize = 10)
    }
  }
}
