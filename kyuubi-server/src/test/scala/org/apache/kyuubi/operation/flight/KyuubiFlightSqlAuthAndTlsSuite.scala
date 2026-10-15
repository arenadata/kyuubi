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

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{FlightSqlTestHelper, KerberizedTestHelper, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.metadata.jdbc.DatabaseType
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.service.authentication.WithLdapServer

class KyuubiFlightSqlAuthAndTlsSuite
  extends WithFlightSqlServer with KerberizedTestHelper with WithLdapServer
    with FlightSqlTestHelper {

  private val currentUser = UserGroupInformation.getCurrentUser
  private val wrongLdapPassword = "wrong-password"

  override def afterAll(): Unit = {
    System.clearProperty("java.security.krb5.conf")
    UserGroupInformation.setLoginUser(currentUser)
    UserGroupInformation.setConfiguration(new Configuration())
    assert(!UserGroupInformation.isSecurityEnabled)
    super.afterAll()
  }

  override protected lazy val conf: KyuubiConf = {
    val config = new Configuration()
    config.set("hadoop.security.authentication", "KERBEROS")
    config.set("hadoop.security.auth_to_local", "DEFAULT RULE:[1:$1] RULE:[2:$1]")
    System.setProperty("java.security.krb5.conf", krb5ConfPath)
    UserGroupInformation.setConfiguration(config)
    assert(UserGroupInformation.isSecurityEnabled)

    val dbPath = Files.createTempFile("kyuubi-flight-auth-it", ".db").toAbsolutePath.toString

    KyuubiConf()
      .set(ENGINE_SHARE_LEVEL, "connection")
      .set(AUTHENTICATION_METHOD, Seq("KERBEROS", "LDAP"))
      .set(SERVER_KEYTAB, testKeytab)
      .set(SERVER_PRINCIPAL, testPrincipal)
      .set(SERVER_SPNEGO_KEYTAB, testKeytab)
      .set(SERVER_SPNEGO_PRINCIPAL, testSpnegoPrincipal)
      .set(AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(AUTHENTICATION_LDAP_BASE_DN, ldapBaseDn.head)
      .set(METADATA_STORE_JDBC_DATABASE_TYPE, DatabaseType.SQLITE.toString)
      .set(METADATA_STORE_JDBC_URL, s"jdbc:sqlite:$dbPath")
      .set(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT, true)
  }

  test("LDAP: Basic auth runs SQL as ldap user") {
    withFlightSqlClient() { (flightClient, sqlClient) =>
      val auth = authenticateBasic(flightClient, ldapUser, ldapUserPasswd)
      val rows = executeAndCollect(sqlClient, "SELECT current_user()", auth)
      assert(rows.size === 1)
      assert(rows.head.head.toString === ldapUser)
    }
  }

  test("LDAP: wrong password is rejected") {
    withFlightSqlClient() { (flightClient, _) =>
      intercept[Exception] {
        flightClient.authenticateBasicToken(ldapUser, wrongLdapPassword)
      }
    }
  }

  test("Bearer token issued by Basic auth can be reused") {
    withFlightSqlClient() { (flightClient, sqlClient) =>
      val bearer = authenticateBasic(flightClient, ldapUser, ldapUserPasswd)
      val rows = executeAndCollect(sqlClient, "SELECT 1", bearer)
      assert(rows.size === 1)
      assert(rows.head.head.toString === "1")
    }
  }

  test("Kerberos: Negotiate auth runs SQL as kerberos user") {
    UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)
    val factory = newBearerCaptureFactory()
    withFlightSqlClient(middleware = Seq(factory)) { (flightClient, sqlClient) =>
      val token = generateToken(hostName)
      val auth = authenticateNegotiate(flightClient, factory, token)
      val rows = executeAndCollect(sqlClient, "SELECT current_user()", auth)
      assert(rows.size === 1)
      assert(rows.head.head.toString === clientPrincipalUser)
    }
  }

  test("no auth is rejected when authentication is required") {
    withFlightSqlClient() { (_, sqlClient) =>
      intercept[Exception] {
        sqlClient.execute("SELECT 1")
      }
    }
  }
}
