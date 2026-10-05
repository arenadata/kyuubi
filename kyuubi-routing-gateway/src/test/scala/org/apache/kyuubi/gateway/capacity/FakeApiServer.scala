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

package org.apache.kyuubi.gateway.capacity

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import io.fabric8.kubernetes.client.ConfigBuilder

/**
 * An api-server that enforces resourceVersion, and nothing else.
 *
 * The store under test relies on one behaviour of Kubernetes: a write carrying
 * a stale resourceVersion is rejected with 409. That is the whole mechanism, so
 * it is the one thing a test must not fake away - a mock that accepts every
 * write would pass while the store silently overwrote concurrent admissions.
 */
class FakeApiServer {

  private val mapper = new ObjectMapper()
  private val objects = new ConcurrentHashMap[String, ObjectNode]()
  private val version = new AtomicInteger(100)

  /** Requests that were rejected for a stale resourceVersion. */
  val conflicts = new AtomicInteger(0)

  /** Every request, as method and path, for diagnosing what the client does. */
  val requests = new java.util.concurrent.ConcurrentLinkedQueue[String]()

  /** Runs before each write, to wedge a concurrent change in. */
  @volatile var beforeWrite: () => Unit = () => ()

  private val server: HttpServer = {
    val s = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    s.createContext("/", (exchange: HttpExchange) => handle(exchange))
    // A pool, not the default single thread: `beforeWrite` makes another
    // request from inside a handler to stage the interleaving under test, and a
    // one-thread server would deadlock on itself rather than run the test.
    s.setExecutor(java.util.concurrent.Executors.newCachedThreadPool())
    s.start()
    s
  }

  def client: KubernetesClient =
    new KubernetesClientBuilder()
      .withConfig(
        new ConfigBuilder()
          .withMasterUrl(s"http://localhost:${server.getAddress.getPort}")
          .withTrustCerts(true)
          .withNamespace("gw")
          .build())
      .build()

  def stop(): Unit = {
    server.stop(0)
    server.getExecutor match {
      case pool: java.util.concurrent.ExecutorService => pool.shutdownNow()
      case _ =>
    }
  }

  def contents: Map[String, ObjectNode] = objects.asScala.toMap

  /** Writes directly, as another replica would. */
  def put(name: String, data: String): Unit = {
    val node = mapper.createObjectNode()
    val meta = node.putObject("metadata")
    meta.put("name", name)
    meta.put("namespace", "gw")
    meta.put("resourceVersion", version.incrementAndGet().toString)
    node.put("kind", "Secret")
    node.put("apiVersion", "v1")
    node.putObject("data").put(
      SecretReservationStore.DataKey,
      java.util.Base64.getEncoder.encodeToString(data.getBytes(UTF_8)))
    objects.put(name, node)
  }

  private def handle(exchange: HttpExchange): Unit = {
    val path = exchange.getRequestURI.getPath
    val method = exchange.getRequestMethod
    val body = new String(exchange.getRequestBody.readAllBytes(), UTF_8)
    requests.add(s"$method $path")

    val (status, response) =
      if (!path.contains("/secrets")) {
        (200, """{"kind":"APIVersions","versions":["v1"]}""")
      } else {
        val name = path.split("/secrets/", 2) match {
          case Array(_, n) if n.nonEmpty => Some(n.split("\\?").head)
          case _ => None
        }
        (method, name) match {
          case ("GET", Some(n)) => Option(objects.get(n))
              .map(o => (200, o.toString))
              .getOrElse((404, notFound(n)))
          case ("GET", None) => (200, list())
          case ("DELETE", Some(n)) =>
            objects.remove(n)
            (200, """{"kind":"Status","status":"Success"}""")
          case ("POST", None) => create(body)
          case ("PUT", Some(n)) => replace(n, body)
          case _ => (405, notFound("method"))
        }
      }

    val bytes = response.getBytes(UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length.toLong)
    exchange.getResponseBody.write(bytes)
    exchange.close()
  }

  // Writes are serialised against each other. A real api server applies them one
  // at a time through etcd, and a double that checked and then wrote without
  // holding anything would lose races of its own - passing every write through
  // while the store under test believed it had won a compare-and-swap.
  private val writeLock = new Object

  private def create(body: String): (Int, String) = {
    beforeWrite()
    val node = mapper.readTree(body).asInstanceOf[ObjectNode]
    val name = node.path("metadata").path("name").asText()
    writeLock.synchronized {
      if (objects.containsKey(name)) {
        conflicts.incrementAndGet()
        (409, conflict(name))
      } else {
        encodeStringData(node)
        stamp(node)
        objects.put(name, node)
        (201, node.toString)
      }
    }
  }

  private def replace(name: String, body: String): (Int, String) = {
    beforeWrite()
    val node = mapper.readTree(body).asInstanceOf[ObjectNode]
    val sent = node.path("metadata").path("resourceVersion").asText("")
    writeLock.synchronized {
      Option(objects.get(name)) match {
        case None => (404, notFound(name))
        case Some(current) if current.path("metadata").path("resourceVersion").asText() != sent =>
          conflicts.incrementAndGet()
          (409, conflict(name))
        case Some(_) =>
          encodeStringData(node)
          stamp(node)
          objects.put(name, node)
          (200, node.toString)
      }
    }
  }

  private def stamp(node: ObjectNode): Unit =
    node.`with`("metadata").put("resourceVersion", version.incrementAndGet().toString)

  /**
   * Folds stringData into data, base64-encoded.
   *
   * The real api server does this on the way in, and a fake that skipped it
   * would let the store write one field and read another - so the round trip
   * would only work here.
   */
  private def encodeStringData(node: ObjectNode): Unit = {
    val stringData = node.get("stringData")
    if (stringData != null && stringData.isObject) {
      val data = node.`with`("data")
      stringData.fields().asScala.foreach { entry =>
        data.put(
          entry.getKey,
          java.util.Base64.getEncoder.encodeToString(entry.getValue.asText().getBytes(UTF_8)))
      }
      node.remove("stringData")
    }
  }

  private def list(): String = {
    val root = mapper.createObjectNode()
    root.put("kind", "SecretList")
    root.put("apiVersion", "v1")
    root.putObject("metadata").put("resourceVersion", version.get().toString)
    val items = root.putArray("items")
    objects.values().asScala.foreach(items.add)
    root.toString
  }

  private def conflict(name: String): String =
    s"""{"kind":"Status","status":"Failure","reason":"Conflict","code":409,
       | "message":"Operation cannot be fulfilled on secrets \\"$name\\":
       | the object has been modified"}""".stripMargin.replace("\n", " ")

  private def notFound(name: String): String =
    s"""{"kind":"Status","status":"Failure","reason":"NotFound","code":404,
       | "message":"secrets \\"$name\\" not found"}""".stripMargin.replace("\n", " ")
}
