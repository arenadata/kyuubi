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

package org.apache.kyuubi.gateway

import java.net.Socket

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.ServiceState

/**
 * Proves the gateway actually starts and listens, rather than merely compiling.
 *
 * Everything else in this module is unit-level; without this the first time
 * anyone learns whether the composition works would be on a cluster.
 */
class RoutingGatewayStartupSuite extends KyuubiFunSuite {

  private def conf: KyuubiConf = KyuubiConf()
    .set(KyuubiConf.FRONTEND_PROTOCOLS.key, "THRIFT_BINARY")
    .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT.key, "0")
    .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_HOST.key, "localhost")
    .set("kyuubi.gateway.cluster.demo.url", "http://trino-demo:8080")
    .set("kyuubi.gateway.cluster.demo.default", "true")

  test("starts, listens on the thrift frontend, and stops") {
    val gateway = new RoutingGateway()
    try {
      gateway.initialize(conf)
      gateway.start()

      assert(gateway.getServiceState === ServiceState.STARTED)
      assert(gateway.frontendServices.size === 1)

      val port = gateway.frontendServices.head.connectionUrl.split(":").last.toInt
      assert(port > 0, "port 0 must have been resolved to a real one")

      val socket = new Socket("localhost", port)
      try assert(socket.isConnected, "the frontend accepts connections")
      finally socket.close()
    } finally {
      gateway.stop()
    }
    assert(gateway.getServiceState === ServiceState.STOPPED)
  }

  test("refuses to start with no supported frontend rather than starting deaf") {
    val deaf = new RoutingGateway()
    val bad = conf.set(KyuubiConf.FRONTEND_PROTOCOLS.key, "REST")
    val e = intercept[IllegalArgumentException](
      try {
        deaf.initialize(bad)
        deaf.start()
      } finally deaf.stop())
    assert(e.getMessage.contains(KyuubiConf.FRONTEND_PROTOCOLS.key))
  }
}
