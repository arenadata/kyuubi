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

import java.io.File

import scala.sys.process._

import org.apache.kyuubi.{FlightSqlTestHelper, Utils, WithFlightSqlServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class KyuubiFlightSqlTlsSuite extends WithFlightSqlServer with FlightSqlTestHelper {

  private val certDir: File = Utils.createTempDir("flight-tls").toFile
  private val certFile: File = new File(certDir, "server.crt")
  private val keyFile: File = new File(certDir, "server.key")

  override protected lazy val conf: KyuubiConf = {
    generatePemMaterial()
    KyuubiConf()
      .set(FRONTEND_FLIGHT_SQL_SSL_ENABLED, true)
      .set(FRONTEND_FLIGHT_SQL_SSL_CERT_FILE, certFile.getAbsolutePath)
      .set(FRONTEND_FLIGHT_SQL_SSL_KEY_FILE, keyFile.getAbsolutePath)
  }

  private def generatePemMaterial(): Unit = {
    val cmd = Seq(
      "openssl",
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-keyout",
      keyFile.getAbsolutePath,
      "-out",
      certFile.getAbsolutePath,
      "-days",
      "1",
      "-nodes",
      "-subj",
      "/CN=localhost")
    val code = Process(cmd).!(ProcessLogger(_ => (), _ => ()))
    assert(code === 0, "openssl failed to generate Flight TLS material")
    assert(certFile.isFile && keyFile.isFile)
  }

  test("TLS: trusted client can execute SQL") {
    withFlightSqlClient(trustedCert = Some(certFile)) { (_, sqlClient) =>
      val rows = executeAndCollect(sqlClient, "SELECT 1")
      assert(rows.size === 1)
      assert(rows.head.head.toString === "1")
    }
  }

  test("TLS: plaintext client is rejected") {
    intercept[Exception] {
      withFlightSqlClient() { (_, sqlClient) =>
        sqlClient.execute("SELECT 1")
      }
    }
  }
}
