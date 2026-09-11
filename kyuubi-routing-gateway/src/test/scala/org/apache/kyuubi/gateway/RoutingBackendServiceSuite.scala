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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.gateway.cluster.StaticClusterResolver
import org.apache.kyuubi.gateway.session.{JdbcRoutingSessionManager, TrinoRoutingSessionManager}

class RoutingBackendServiceSuite extends KyuubiFunSuite {

  private val resolver = new StaticClusterResolver(Map.empty)

  private def managerFor(engine: String) =
    RoutingBackendService.sessionManagerFor(
      KyuubiConf().set(RoutingBackendService.ENGINE_KEY, engine),
      resolver,
      None)

  test("trino is served by the trino session manager") {
    assert(managerFor("trino").isInstanceOf[TrinoRoutingSessionManager])
  }

  test("an engine a dialect declares is served over jdbc") {
    val manager = managerFor("impala")
    assert(manager.isInstanceOf[JdbcRoutingSessionManager])
    assert(manager.engine === "impala")
  }

  test("a Spark Thrift Server is served over jdbc like any other HS2 backend") {
    val manager = managerFor("spark")
    assert(manager.isInstanceOf[JdbcRoutingSessionManager])
    assert(manager.engine === "spark")
  }

  test("an engine nobody can serve is a startup error naming what is supported") {
    val e = intercept[IllegalArgumentException](managerFor("cassandra"))
    assert(e.getMessage.contains("cassandra"))
    assert(e.getMessage.contains("trino"), "the message should say what is supported")
    assert(e.getMessage.contains("impala"))
    assert(e.getMessage.contains("spark"))
  }

  test("a typo is refused rather than taken for a jdbc engine") {
    // Left to the dialect lookup this came back at the first session as
    // "Don't find jdbc dialect implement for jdbc engine: trno", from a gateway
    // that had already reported itself healthy.
    intercept[IllegalArgumentException](managerFor("trno"))
  }

  test("the engines on offer come from the dialects, not from a list here") {
    val engines = RoutingBackendService.jdbcEngines
    assert(engines.contains("impala"), "shipped by the JDBC engine")
    assert(engines.contains("spark"), "added by this module's own dialect")
    assert(!engines.contains("cassandra"))
    assert(
      engines.forall(e => e == e.toLowerCase),
      "matched case-insensitively, so stored lowercase")
  }
}
