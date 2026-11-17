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

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.ServiceState._
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

class KyuubiFlightSqlFrontendServiceSuite extends KyuubiFunSuite {

  test("Flight SQL frontend lifecycle") {
    val server = new KyuubiServer
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
      .set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
      .set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)

    assert(server.getServiceState === LATENT)
    server.initialize(conf)
    assert(server.getServiceState === INITIALIZED)
    assert(server.frontendServices.size === 1)
    val frontend = server.frontendServices.head
    assert(frontend.getServiceState === INITIALIZED)
    assert(frontend.connectionUrl.startsWith("localhost:"))

    server.start()
    assert(server.getServiceState === STARTED)
    assert(frontend.getServiceState === STARTED)
    assert(frontend.connectionUrl.matches("localhost:[0-9]+"))

    server.stop()
    assert(server.getServiceState === STOPPED)
    assert(frontend.getServiceState === STOPPED)
    server.stop()
  }

  test("Flight SQL frontend keeps a non-daemon server thread") {
    val server = new KyuubiServer
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
      .set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
      .set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)

    try {
      server.initialize(conf)
      server.start()
      val keeper = flightSqlServerThread
      assert(keeper.isDefined, "expected a live non-daemon Flight SQL server thread")
      assert(!keeper.get.isDaemon)
      assert(keeper.get.isAlive)
    } finally {
      server.stop()
    }
    assert(flightSqlServerThread.isEmpty)
  }

  test("Flight SQL advertised host") {
    val server = new KyuubiServer
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
      .set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
      .set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)
      .set(FRONTEND_ADVERTISED_HOST, "flight.example")

    try {
      server.initialize(conf)
      assert(server.frontendServices.head.connectionUrl.startsWith("flight.example:"))
    } finally {
      server.stop()
    }
  }

  test("Flight SQL wildcard bind host is not advertised") {
    Seq("0.0.0.0", "::", "[::]", "0:0:0:0:0:0:0:0", "*").foreach { h =>
      assert(KyuubiFlightSqlFrontendService.isWildcard(h), h)
    }
    Seq("localhost", "127.0.0.1", "::1", "flight.example", "10.0.0.1").foreach { h =>
      assert(!KyuubiFlightSqlFrontendService.isWildcard(h), h)
    }

    val server = new KyuubiServer
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
      .set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "0.0.0.0")
      .set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)

    try {
      server.initialize(conf)
      server.start()
      val url = server.frontendServices.head.connectionUrl
      assert(!url.startsWith("0.0.0.0:"), url)
      assert(url.matches(".+:[0-9]+"), url)
    } finally {
      server.stop()
    }
  }

  test("Flight SQL TLS without certificate material fails startup") {
    val server = new KyuubiServer
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
      .set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
      .set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)
      .set(FRONTEND_FLIGHT_SQL_SSL_ENABLED, true)

    try {
      server.initialize(conf)
      intercept[Exception] {
        server.start()
      }
    } finally {
      server.stop()
    }
  }

  test("Flight SQL as the only frontend initializes the engine security accessor") {
    val server = new KyuubiServer
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
      .set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
      .set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)
      .set(ENGINE_SECURITY_ENABLED, true)
      .set(ENGINE_SECURITY_SECRET_PROVIDER, "simple")
      .set(SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET, "flight-sql-test-secret")

    InternalSecurityAccessor.reset()
    try {
      server.initialize(conf)
      // KyuubiSessionImpl issues the engine token with it when it launches an engine
      assert(InternalSecurityAccessor.get() != null)
      assert(InternalSecurityAccessor.get().issueToken().nonEmpty)
    } finally {
      server.stop()
      InternalSecurityAccessor.reset()
    }
  }

  private def flightSqlServerThread: Option[Thread] =
    Thread.getAllStackTraces.keySet.asScala.find { t =>
      t.isAlive && !t.isDaemon && t.getName.startsWith("KyuubiFlightSqlFrontendService")
    }
}
