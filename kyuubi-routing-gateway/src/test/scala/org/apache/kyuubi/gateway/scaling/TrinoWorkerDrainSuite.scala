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

package org.apache.kyuubi.gateway.scaling

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import okhttp3.OkHttpClient

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.gateway.cluster.ClusterRef

/**
 * A worker's `/v1/info/state` as Trino guards it: reading the state is public,
 * changing it is a management write that wants an identity even when no
 * authentication is configured.
 */
class TrinoWorkerDrainSuite extends KyuubiFunSuite {

  private class FakeWorker(acceptsUser: String) {
    @volatile var state: String = TrinoWorkerDrain.Active
    @volatile var lastUser: Option[String] = None

    private val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/v1/info/state", (exchange: HttpExchange) => handle(exchange))
    server.start()

    def url: String = s"http://localhost:${server.getAddress.getPort}"

    def stop(): Unit = server.stop(0)

    private def handle(exchange: HttpExchange): Unit = {
      val user = Option(exchange.getRequestHeaders.getFirst(TrinoWorkerDrain.UserHeader))
      exchange.getRequestMethod match {
        case "GET" =>
          respond(exchange, 200, "\"" + state + "\"")
        case "PUT" =>
          lastUser = user
          if (!user.contains(acceptsUser)) {
            respond(
              exchange,
              401,
              "Basic authentication or X-Trino-Original-User or X-Trino-User must be sent")
          } else {
            state = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
              .trim.stripPrefix("\"").stripSuffix("\"")
            respond(exchange, 200, "OK")
          }
        case other =>
          respond(exchange, 405, s"$other not allowed")
      }
    }

    private def respond(exchange: HttpExchange, code: Int, body: String): Unit = {
      val bytes = body.getBytes(StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(code, bytes.length)
      exchange.getResponseBody.write(bytes)
      exchange.close()
    }
  }

  private def withWorker(acceptsUser: String)(f: FakeWorker => Unit): Unit = {
    val worker = new FakeWorker(acceptsUser)
    try f(worker)
    finally worker.stop()
  }

  private val cluster = ClusterRef("c", "trino", "http://c:8080")

  private def drainAs(user: String): TrinoWorkerDrain =
    new TrinoWorkerDrain(_ => new OkHttpClient.Builder().build(), user)

  test("a drain speaks as the configured Trino user") {
    withWorker(acceptsUser = "gateway") { worker =>
      val drain = drainAs("gateway")
      drain.drain(cluster, worker.url)
      assert(worker.lastUser === Some("gateway"))
      assert(worker.state === TrinoWorkerDrain.Draining)
      assert(drain.state(cluster, worker.url) === Some(TrinoWorkerDrain.Draining))
      drain.undrain(cluster, worker.url)
      assert(worker.state === TrinoWorkerDrain.Active)
    }
  }

  test("a refused transition says which code and why") {
    withWorker(acceptsUser = "someone-else") { worker =>
      val e = intercept[IllegalStateException](drainAs("gateway").drain(cluster, worker.url))
      assert(e.getMessage.contains("HTTP 401"))
      assert(e.getMessage.contains("X-Trino-User"))
      assert(worker.state === TrinoWorkerDrain.Active)
    }
  }

  test("each worker is reached with the client built for its cluster") {
    withWorker(acceptsUser = "gateway") { worker =>
      var clientsFor = Seq.empty[ClusterRef]
      val client = new OkHttpClient.Builder().build()
      val drain = new TrinoWorkerDrain(c => { clientsFor :+= c; client }, "gateway")

      drain.drain(cluster, worker.url)

      assert(clientsFor === Seq(cluster))
    }
  }
}
