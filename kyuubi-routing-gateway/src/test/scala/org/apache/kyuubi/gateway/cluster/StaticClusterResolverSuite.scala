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

package org.apache.kyuubi.gateway.cluster

import org.apache.kyuubi.KyuubiFunSuite

class StaticClusterResolverSuite extends KyuubiFunSuite {

  private val conf = Map(
    "kyuubi.gateway.cluster.teamA.url" -> "http://trino-a:8080",
    "kyuubi.gateway.cluster.teamA.users" -> "alice, bob",
    "kyuubi.gateway.cluster.teamA.session.trino.catalog" -> "hive",
    "kyuubi.gateway.cluster.shared.url" -> "http://trino-shared:8080",
    "kyuubi.gateway.cluster.shared.default" -> "true",
    "unrelated.key" -> "ignored")

  test("parses clusters and ignores unrelated keys") {
    val resolver = new StaticClusterResolver(conf)
    assert(resolver.clusters.map(_.name).toSet === Set("teamA", "shared"))
    val a = resolver.clusters.find(_.name == "teamA").get
    assert(a.url === "http://trino-a:8080")
    assert(a.engine === "trino", "engine defaults to trino")
    assert(a.users === Set("alice", "bob"), "user list is trimmed")
    assert(a.sessionConf === Map("trino.catalog" -> "hive"))
    assert(!a.isDefault)
  }

  test("routes a known user to their cluster") {
    val resolver = new StaticClusterResolver(conf)
    assert(resolver.resolve("alice", Map.empty).map(_.name) === Some("teamA"))
  }

  test("falls back to the default cluster for an unmapped user") {
    val resolver = new StaticClusterResolver(conf)
    assert(resolver.resolve("carol", Map.empty).map(_.name) === Some("shared"))
  }

  test("rejects when no cluster is allowed and none is default") {
    val resolver = new StaticClusterResolver(conf - "kyuubi.gateway.cluster.shared.default")
    assert(resolver.resolve("carol", Map.empty).isEmpty,
      "an unmapped user must be rejected, not silently sent somewhere")
  }

  test("a cluster without a url is not a cluster") {
    val resolver = new StaticClusterResolver(Map("kyuubi.gateway.cluster.broken.engine" -> "trino"))
    assert(resolver.clusters.isEmpty)
  }
}
