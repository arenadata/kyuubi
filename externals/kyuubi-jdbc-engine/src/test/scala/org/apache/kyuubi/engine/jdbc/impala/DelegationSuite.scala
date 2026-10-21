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

import java.sql.DriverManager

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.HiveJDBCTestHelper

/**
 * Impala delegates a session to another user only on the OpenSession key impala.doas.user
 * (impala-compose.yml lets kyuubi delegate to alice and bob only).
 */
class DelegationSuite extends WithImpalaEngine with HiveJDBCTestHelper {

  // the engine logs in as kyuubi and asks Impala to run its sessions as alice, as a connection
  // url ending in impala.doas.user={user} does once {user} is filled in
  override def withKyuubiConf: Map[String, String] = super.withKyuubiConf ++ Map(
    ENGINE_JDBC_CONNECTION_USER.key -> "kyuubi",
    ENGINE_JDBC_CONNECTION_URL.key -> s"$hiveServerJdbcUrl;impala.doas.user=alice")

  override protected def jdbcUrl: String = jdbcConnectionUrl

  /** user() and effective_user() in Impala, for a connection to `url` as `user`. */
  private def impalaUsers(url: String, user: String): (String, String) = {
    val conn = DriverManager.getConnection(url, user, "")
    try {
      val rs = conn.createStatement().executeQuery("select user(), effective_user()")
      assert(rs.next())
      (rs.getString(1), rs.getString(2))
    } finally {
      conn.close()
    }
  }

  test("the driver sends impala.doas.user from the URL to Impala") {
    assert(impalaUsers(hiveServerJdbcUrl, "kyuubi") === ("kyuubi", "kyuubi"))
    assert(impalaUsers(s"$hiveServerJdbcUrl;impala.doas.user=bob", "kyuubi") ===
      ("kyuubi", "bob"))
  }

  test("Impala refuses a delegation its proxy user config does not allow") {
    val e = intercept[java.sql.SQLException] {
      impalaUsers(s"$hiveServerJdbcUrl;impala.doas.user=carol", "kyuubi")
    }
    assert(e.getMessage.contains("is not authorized to delegate to 'carol'"))
  }

  test("an engine connection url with impala.doas.user runs the session as that user") {
    assert(impalaUsers(jdbcConnectionUrl, "anyone") === ("kyuubi", "alice"))
  }
}
