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

package org.apache.kyuubi.gateway.sizing

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.ZoneId
import java.util.{Locale, Optional}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.trino.client.ClientSession
import okhttp3.OkHttpClient

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.TrinoContext

/**
 * Drives the planner against a coordinator that speaks the client protocol.
 *
 * A stubbed QueryPlanner already covers what the gate does with an estimate;
 * what only a real exchange can show is that the planner asks for the plan it
 * means to ask for and reads back the document the parser expects. Faking the
 * coordinator rather than running one keeps that check in the unit suite.
 */
class TrinoQueryPlannerSuite extends KyuubiFunSuite {

  private var server: HttpServer = _
  private val received = new AtomicReference[String]("")

  private val plan =
    """{"id":"0","estimates":[{"memoryCost":1024.0,"cpuCost":2048.0}],"children":[]}"""

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/v1/statement", (exchange: HttpExchange) => respond(exchange))
    server.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
    super.afterAll()
  }

  private def respond(exchange: HttpExchange): Unit = {
    received.set(new String(exchange.getRequestBody.readAllBytes(), UTF_8))
    // A complete single-page QueryResults: no nextUri, so the client finishes
    // on the first response and the plan arrives as one row of one column.
    val body =
      s"""{"id":"20260911_000000_00000_aaaaa",
         | "infoUri":"http://localhost/ui/query.html",
         | "columns":[{"name":"Query Plan","type":"varchar",
         |             "typeSignature":{"rawType":"varchar","arguments":[]}}],
         | "data":[[${quote(plan)}]],
         | "stats":{"state":"FINISHED","queued":false,"scheduled":true,
         |          "nodes":1,"totalSplits":1,"queuedSplits":0,"runningSplits":0,
         |          "completedSplits":1,"cpuTimeMillis":1,"wallTimeMillis":1,
         |          "queuedTimeMillis":0,"elapsedTimeMillis":1,"processedRows":1,
         |          "processedBytes":1,"physicalInputBytes":1,"physicalWrittenBytes":0,
         |          "peakMemoryBytes":1,"spilledBytes":0},
         | "warnings":[]}""".stripMargin
    val bytes = body.getBytes(UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(200, bytes.length.toLong)
    exchange.getResponseBody.write(bytes)
    exchange.close()
  }

  private def quote(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private def planner: TrinoQueryPlanner = {
    val session = ClientSession.builder()
      .server(URI.create(s"http://localhost:${server.getAddress.getPort}"))
      .principal(Optional.of("alice"))
      .source("kyuubi")
      .catalog("hive")
      .schema("default")
      .timeZone(ZoneId.of("UTC"))
      .locale(Locale.ENGLISH)
      .properties(Map.empty[String, String].asJava)
      .build()
    new TrinoQueryPlanner(
      TrinoContext(new OkHttpClient.Builder().build(), session),
      KyuubiConf())
  }

  test("asks the coordinator for the distributed plan and reads it back") {
    val result = planner.explain("SELECT * FROM t")
    assert(received.get() === "EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) SELECT * FROM t")
    assert(result === plan)

    // The point of asking: the estimate the gate will size against.
    val estimate = ExplainParser.parse(result)
    assert(estimate.peakMemoryBytes === 1024L)
  }

  test("a trailing semicolon does not become part of the explained statement") {
    planner.explain("SELECT 1;  ")
    assert(received.get() === "EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) SELECT 1")
  }

  test("an empty result is not mistaken for a plan") {
    assert(TrinoQueryPlanner.firstCell(Iterator.empty) === "")
    assert(TrinoQueryPlanner.firstCell(Iterator(List(null))) === "")
  }
}
