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

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.session.TrinoSessionImpl
import org.apache.kyuubi.operation.TClientTestUtils
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TCloseSessionReq, TOpenSessionReq, TStatusCode}

/**
 * Drives the gateway over real HS2 to prove that identity decides the cluster.
 *
 * No Trino is reachable here, and none is needed: [[org.apache.kyuubi.engine.trino
 * .session.TrinoSessionImpl.open]] builds the client session without a round
 * trip, so opening a session exercises the whole resolve-and-route path -
 * frontend, backend, session manager, resolver - and stops just short of the
 * network. Whether the routed URL is reachable is the cluster's business, not
 * the gateway's.
 */
class RoutingEndToEndSuite extends KyuubiFunSuite {

  private var gateway: RoutingGateway = _
  private var url: String = _

  override def beforeAll(): Unit = {
    val conf = KyuubiConf()
      .set(KyuubiConf.FRONTEND_PROTOCOLS.key, "THRIFT_BINARY")
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT.key, "0")
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_HOST.key, "localhost")
      // Two clusters, disjoint membership, no default - so an unmapped user has
      // nowhere to land and must be refused rather than quietly admitted.
      .set("kyuubi.gateway.cluster.analytics.url", "http://trino-analytics:8080")
      .set("kyuubi.gateway.cluster.analytics.users", "alice")
      .set(
        "kyuubi.gateway.cluster.analytics.session.kyuubi.session.engine.trino.connection.catalog",
        "hive")
      .set("kyuubi.gateway.cluster.etl.url", "http://trino-etl:8080")
      .set("kyuubi.gateway.cluster.etl.users", "bob")
      .set(
        "kyuubi.gateway.cluster.etl.session.kyuubi.session.engine.trino.connection.catalog",
        "hive")

    gateway = new RoutingGateway()
    gateway.initialize(conf)
    gateway.start()
    url = gateway.frontendServices.head.connectionUrl
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (gateway != null) gateway.stop()
    super.afterAll()
  }

  private def openAs(user: String): (TStatusCode, String) = {
    TClientTestUtils.withThriftClient(url, Some(user)) { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      req.setConfiguration(Map.empty[String, String].asJava)
      val resp = client.OpenSession(req)
      val status = resp.getStatus
      if (status.getStatusCode == TStatusCode.SUCCESS_STATUS) {
        client.CloseSession(new TCloseSessionReq(resp.getSessionHandle))
      }
      (status.getStatusCode, Option(status.getErrorMessage).getOrElse(""))
    }
  }

  test("an authorised user reaches their own cluster") {
    val (code, message) = openAs("alice")
    assert(code === TStatusCode.SUCCESS_STATUS, s"alice was refused: $message")
  }

  test("the query reaches the cluster as its caller, not as the gateway") {
    TClientTestUtils.withThriftClient(url, Some("alice")) { client =>
      val req = new TOpenSessionReq()
      req.setUsername("alice")
      req.setPassword("anonymous")
      req.setConfiguration(Map.empty[String, String].asJava)
      val resp = client.OpenSession(req)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      try {
        val handle = SessionHandle(resp.getSessionHandle)
        val session = gateway.backendService.sessionManager.getSession(handle)
          .asInstanceOf[TrinoSessionImpl]

        // Without an explicit session user the Trino client falls back to the
        // OS user of this process, and every caller would share one identity at
        // the cluster - so assert on the principal actually going out, not just
        // on the conf that produces it.
        val principal = session.trinoContext.clientSession.get.getPrincipal
        assert(principal.isPresent && principal.get === "alice")
        assert(session.trinoContext.clientSession.get.getServer.toString
          === "http://trino-analytics:8080")
      } finally {
        client.CloseSession(new TCloseSessionReq(resp.getSessionHandle))
      }
    }
  }

  test("routing follows identity, not the connection") {
    TClientTestUtils.withThriftClient(url, Some("bob")) { client =>
      val req = new TOpenSessionReq()
      req.setUsername("bob")
      req.setPassword("anonymous")
      req.setConfiguration(Map.empty[String, String].asJava)
      val resp = client.OpenSession(req)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      try {
        val handle = SessionHandle(resp.getSessionHandle)
        val session = gateway.backendService.sessionManager.getSession(handle)
          .asInstanceOf[TrinoSessionImpl]
        assert(session.trinoContext.clientSession.get.getServer.toString
          === "http://trino-etl:8080")
      } finally {
        client.CloseSession(new TCloseSessionReq(resp.getSessionHandle))
      }
    }
  }

  test("a user with no cluster is refused rather than sent to the default") {
    val (code, message) = openAs("carol")
    assert(code === TStatusCode.ERROR_STATUS)
    assert(
      message.contains("No cluster is allowed for user carol"),
      s"the refusal must name the reason, got: $message")
  }
}
