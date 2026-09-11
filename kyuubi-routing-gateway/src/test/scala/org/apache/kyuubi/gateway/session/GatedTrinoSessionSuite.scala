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

package org.apache.kyuubi.gateway.session

import java.net.URI
import java.time.ZoneId
import java.util.{Locale, Optional}

import scala.collection.JavaConverters._

import io.trino.client.ClientSession

import org.apache.kyuubi.KyuubiFunSuite

class GatedTrinoSessionSuite extends KyuubiFunSuite {

  private def session(properties: Map[String, String] = Map.empty): ClientSession =
    ClientSession.builder()
      .server(URI.create("http://trino:8080"))
      .principal(Optional.of("alice"))
      .source("kyuubi")
      .catalog("hive")
      .schema("default")
      .timeZone(ZoneId.of("UTC"))
      .locale(Locale.ENGLISH)
      .properties(properties.asJava)
      .build()

  test("the admitted worker count reaches the cluster as a session property") {
    val applied = GatedTrinoSession.withRequiredWorkers(session(), 6, None)
    assert(applied.getProperties.get("required_workers_count") === "6")
    assert(!applied.getProperties.containsKey("required_workers_max_wait_time"))
  }

  test("existing session properties survive") {
    val applied = GatedTrinoSession.withRequiredWorkers(
      session(Map("query_max_memory" -> "10GB")),
      2,
      Some("2m"))
    assert(applied.getProperties.get("query_max_memory") === "10GB")
    assert(applied.getProperties.get("required_workers_count") === "2")
    assert(applied.getProperties.get("required_workers_max_wait_time") === "2m")
  }

  test("a later query overrides the requirement of an earlier one") {
    val first = GatedTrinoSession.withRequiredWorkers(session(), 6, None)
    val second = GatedTrinoSession.withRequiredWorkers(first, 2, None)
    assert(
      second.getProperties.get("required_workers_count") === "2",
      "a stale requirement would hold a small query waiting for workers it does not need")
  }
}
