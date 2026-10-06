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

import org.apache.arrow.flight.CallStatus

import org.apache.kyuubi.{FlightSqlTestHelper, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf

class KyuubiFlightSqlOperationSuite extends WithFlightSqlServer with FlightSqlTestHelper {

  override protected val conf: KyuubiConf = KyuubiConf()

  test("multi-column query returns schema and rows") {
    withFlightSqlClient() { (_, sqlClient) =>
      val info = sqlClient.execute("SELECT 1 AS id, 'kyuubi' AS name")
      assert(info.getSchemaOptional.get().getFields.size() === 2)
      assert(info.getSchemaOptional.get().getFields.get(0).getName.equalsIgnoreCase("id"))
      assert(info.getSchemaOptional.get().getFields.get(1).getName.equalsIgnoreCase("name"))
      val rows = readAllRows(sqlClient, info)
      assert(rows.size === 1)
      assert(rows.head(0).toString === "1")
      assert(rows.head(1).toString === "kyuubi")
    }
  }

  test("prepared statement is UNIMPLEMENTED") {
    withFlightSqlClient() { (_, sqlClient) =>
      assertFlightStatus(CallStatus.UNIMPLEMENTED) {
        sqlClient.prepare("SELECT 1")
      }
    }
  }
}
