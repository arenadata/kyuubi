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

package org.apache.kyuubi.operation

import java.nio.file.Files

import scala.sys.process._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.kyuubi.{KyuubiAuthType, KyuubiSessionBuilder}

import org.apache.kyuubi.{KerberizedTestHelper, Utils, WithSparkConnectServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.metadata.jdbc.DatabaseType
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.service.authentication.WithLdapServer

/**
 * Integration tests for Spark Connect authentication via KyuubiSessionBuilder.
 */
class KyuubiSparkConnectKerberosAndPlainAuthSuite
  extends WithSparkConnectServer with KerberizedTestHelper with WithLdapServer {

  private val currentUser = UserGroupInformation.getCurrentUser
  private val wrongLdapPassword = "wrong-password"
  private val tempViewName = "sc_auth_view"
  private val tempViewCol1 = 5
  private val tempViewCol2 = 10

  private def scUrl: String = s"sc://$connectUrl"

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

    val dbPath = Files.createTempFile("kyuubi-sc-auth-it", ".db").toAbsolutePath.toString

    KyuubiConf()
      .set(ENGINE_SHARE_LEVEL, "connection")
      .set(AUTHENTICATION_METHOD, Seq("KERBEROS", "LDAP"))
      .set(SERVER_KEYTAB, testKeytab)
      .set(SERVER_PRINCIPAL, testPrincipal)
      .set(SERVER_SPNEGO_KEYTAB, testKeytab)
      .set(SERVER_SPNEGO_PRINCIPAL, testSpnegoPrincipal)
      .set(AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(AUTHENTICATION_LDAP_BASE_DN, ldapBaseDn.head)
      .set(FRONTEND_SPARK_CONNECT_BIND_HOST.key, "localhost")
      .set(METADATA_STORE_JDBC_DATABASE_TYPE, DatabaseType.SQLITE.toString)
      .set(METADATA_STORE_JDBC_URL, s"jdbc:sqlite:$dbPath")
      .set(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT, true)
  }

  private def exceptionMessage(e: Throwable): String = {
    Option(e.getMessage).getOrElse("") +
      Option(e.getCause).flatMap(c => Option(c.getMessage)).getOrElse("")
  }

  private def assertAuthRejected(e: Throwable): Unit = {
    val msg = exceptionMessage(e).toLowerCase
    assert(
      msg.contains("unauthenticated") ||
        msg.contains("authentication failed") ||
        msg.contains("missing authorization") ||
        msg.contains("unauthorized") ||
        e.getClass.getName.contains("StatusRuntimeException"),
      s"expected auth rejection, got: $e")
  }

  private def withSparkSession(spark: SparkSession)(body: SparkSession => Unit): Unit = {
    try {
      body(spark)
    } finally {
      try spark.stop()
      catch { case NonFatal(_) => }
    }
  }

  private def assertCurrentUserAndTempView(spark: SparkSession, expectedUser: String): Unit = {
    val userRows = spark.sql("SELECT current_user()").collect()
    assert(userRows.length === 1)
    assert(userRows(0).getString(0) === expectedUser)

    spark.sql(
      s"CREATE OR REPLACE TEMP VIEW $tempViewName AS " +
        s"SELECT $tempViewCol1 AS v1, $tempViewCol2 AS v2")
    val dataRows = spark.sql(s"SELECT v1, v2 FROM $tempViewName").collect()
    assert(dataRows.length === 1)
    assert(dataRows(0).getInt(0) === tempViewCol1)
    assert(dataRows(0).getInt(1) === tempViewCol2)
  }

  test("KERBEROS: KyuubiSessionBuilder authenticates and runs SQL as kerberos user") {
    assume(Utils.isCommandAvailable("kinit"))
    val commands = Seq("kinit", "-kt", testKeytab, testPrincipal)
    val kinitProc = new java.lang.ProcessBuilder(commands: _*).inheritIO()
    kinitProc.environment().put("KRB5_CONFIG", krb5ConfPath)
    val ret = kinitProc.start().waitFor()
    assert(ret === 0, "kinit failed")

    try {
      withSparkSession(new KyuubiSessionBuilder(scUrl, KyuubiAuthType.KERBEROS).getOrCreate()) {
        assertCurrentUserAndTempView(_, clientPrincipalUser)
      }
    } finally {
      "kdestroy".!
    }
  }

  test("LDAP: KyuubiSessionBuilder authenticates and runs SQL as ldap user") {
    withSparkSession(
      new KyuubiSessionBuilder(scUrl, KyuubiAuthType.LDAP, ldapUser, ldapUserPasswd).getOrCreate()) {
      assertCurrentUserAndTempView(_, ldapUser)
    }
  }

  test("LDAP: wrong password is rejected") {
    val e = intercept[Exception] {
      new KyuubiSessionBuilder(scUrl, KyuubiAuthType.LDAP, ldapUser, wrongLdapPassword).getOrCreate()
    }
    assertAuthRejected(e)
  }

  test("no auth: plain Spark Connect client is rejected when auth is required") {
    withSparkSession(SparkSession.builder().remote(scUrl).getOrCreate()) { spark =>
      val e = intercept[Exception] {
        spark.sql("SELECT 1").collect()
      }
      assertAuthRejected(e)
    }
  }
}
