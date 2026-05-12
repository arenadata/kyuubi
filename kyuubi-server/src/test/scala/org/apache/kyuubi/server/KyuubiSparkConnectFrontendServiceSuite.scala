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

package org.apache.kyuubi.server

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.{NoopSparkConnectServer, ServiceState}

class KyuubiSparkConnectFrontendServiceSuite extends KyuubiFunSuite {

  private def baseConf(): KyuubiConf = KyuubiConf()
    .set(FRONTEND_SPARK_CONNECT_BIND_HOST.key, "localhost")
    .set(FRONTEND_SPARK_CONNECT_BIND_PORT, Utils.findFreePort())

  test("start/stop lifecycle") {
    val server = new NoopSparkConnectServer
    server.initialize(baseConf().set(AUTHENTICATION_METHOD, Seq("NOSASL")))
    assert(server.getServiceState === ServiceState.INITIALIZED)

    server.start()
    assert(server.getServiceState === ServiceState.STARTED)
    val url = server.frontendServices.head.connectionUrl
    assert(url.startsWith("localhost:"))
    assert(url.split(":").last.toInt > 0)

    server.stop()
    assert(server.getServiceState === ServiceState.STOPPED)
  }

  test("SSL: missing keystore path throws IllegalArgumentException") {
    val server = new NoopSparkConnectServer
    server.initialize(baseConf()
      .set(AUTHENTICATION_METHOD, Seq("NOSASL"))
      .set(FRONTEND_SPARK_CONNECT_SSL_ENABLED, true))

    val e = intercept[KyuubiException](server.start())
    assert(e.getCause.isInstanceOf[IllegalArgumentException])
    assert(e.getCause.getMessage.contains(FRONTEND_SSL_KEYSTORE_PATH.key))
    server.stop()
  }

  test("SSL: missing keystore password throws IllegalArgumentException") {
    val server = new NoopSparkConnectServer
    server.initialize(baseConf()
      .set(AUTHENTICATION_METHOD, Seq("NOSASL"))
      .set(FRONTEND_SPARK_CONNECT_SSL_ENABLED, true)
      .set(FRONTEND_SSL_KEYSTORE_PATH, "/some/path/keystore.jks"))

    val e = intercept[KyuubiException](server.start())
    assert(e.getCause.isInstanceOf[IllegalArgumentException])
    assert(e.getCause.getMessage.contains(FRONTEND_SSL_KEYSTORE_PASSWORD.key))
    server.stop()
  }

  test("NOSASL: initialize succeeds without auth service") {
    val server = new NoopSparkConnectServer
    server.initialize(baseConf().set(AUTHENTICATION_METHOD, Seq("NOSASL")))
    server.start()
    server.stop()
  }

  test("LDAP: initialize succeeds with auth service") {
    val server = new NoopSparkConnectServer
    server.initialize(baseConf()
      .set(AUTHENTICATION_METHOD, Seq("LDAP"))
      .set(AUTHENTICATION_LDAP_URL, "ldap://localhost:9999")
      .set(AUTHENTICATION_LDAP_BASE_DN, "ou=users"))
    server.start()
    server.stop()
  }
}
