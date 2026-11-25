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

package org.apache.kyuubi.server.api

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.SSLContext

import com.sun.net.httpserver.{HttpExchange, HttpsConfigurator, HttpServer, HttpsServer}

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.KeyStoreTestUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

/**
 * The engine UI proxy of a Kyuubi server in front of stub web UIs on localhost. A stub echoes
 * the request URI and the base path announced in X-Forwarded-Context.
 */
abstract class EngineUIProxySuiteBase extends KyuubiFunSuite with RestFrontendTestHelper {

  /** Requests the stub web UIs received, denied requests must not reach them. */
  protected val stubRequests = new AtomicInteger

  /** A proxy enabled for localhost, trusting the test CA. */
  protected def proxyConf: KyuubiConf = KyuubiConf()
    .set(FRONTEND_REST_ENGINE_UI_PROXY_ENABLED, true)
    .set(FRONTEND_REST_ENGINE_UI_PROXY_HOSTS, Seq("localhost"))
    .set(ENGINE_SSL_TRUSTSTORE_PATH, trustStore.toString)
    .set(ENGINE_SSL_TRUSTSTORE_TYPE, storeTypeOf(trustStore))
    .set(ENGINE_SSL_TRUSTSTORE_PASSWORD, passwordOf(trustStore))

  override protected lazy val conf: KyuubiConf = proxyConf

  /** Run `f` with the port of a stub web UI, served over HTTPS when a key store is given. */
  protected def withEngineUI(keyStore: Option[Path])(f: Int => Unit): Unit = {
    val address = new InetSocketAddress("localhost", 0)
    val server = keyStore.map { store =>
      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(keyManagers(store), null, null)
      val https = HttpsServer.create(address, 0)
      https.setHttpsConfigurator(new HttpsConfigurator(sslContext))
      https: HttpServer
    }.getOrElse(HttpServer.create(address, 0))
    server.createContext(
      "/",
      (exchange: HttpExchange) => {
        stubRequests.incrementAndGet()
        val context = exchange.getRequestHeaders.getFirst("X-Forwarded-Context")
        val body = s"${exchange.getRequestURI} $context".getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(200, body.length)
        exchange.getResponseBody.write(body)
        exchange.close()
      })
    server.start()
    try f(server.getAddress.getPort)
    finally server.stop(0)
  }

  /** @return the status and the body of a GET through the proxy */
  protected def proxyGet(path: String): (Int, String) = {
    val response = webTarget.path(path).request().get()
    (response.getStatus, response.readEntity(classOf[String]))
  }
}

class EngineUIProxyTlsSuite extends EngineUIProxySuiteBase {

  test("proxy an engine web UI served over HTTPS") {
    withEngineUI(Some(goodKeyStore)) { port =>
      val base = s"engine-ui/https/localhost:$port"
      val response = webTarget.path(s"$base/stages/").queryParam("id", "7").request().get()
      assert(response.getStatus === 200)
      assert(response.readEntity(classOf[String]) === s"/stages/?id=7 /$base")
      // the empty path is rewritten to the Jobs page
      assert(proxyGet(s"$base/") === (200, s"/jobs/ /$base"))
    }
  }

  test("proxy an engine web UI served over plain HTTP") {
    withEngineUI(None) { port =>
      val base = s"engine-ui/localhost:$port"
      assert(proxyGet(s"$base/jobs/") === (200, s"/jobs/ /$base"))
    }
  }

  test("reject an engine web UI whose certificate is not issued by the trusted CA") {
    withEngineUI(Some(foreignKeyStore)) { port =>
      assert(proxyGet(s"engine-ui/https/localhost:$port/jobs/")._1 === 502)
    }
  }

  test("reject an engine web UI whose certificate is for another host") {
    withEngineUI(Some(otherHostKeyStore)) { port =>
      assert(proxyGet(s"engine-ui/https/localhost:$port/jobs/")._1 === 502)
    }
  }

  test("deny an engine web UI on a host that is not allowed") {
    withEngineUI(Some(goodKeyStore)) { port =>
      val before = stubRequests.get()
      val (status, body) = proxyGet(s"engine-ui/https/127.0.0.1:$port/jobs/")
      assert(status === 403)
      assert(body.contains("Engine UI host is not allowed"))
      assert(body.contains(FRONTEND_REST_ENGINE_UI_PROXY_HOSTS.key))
      assert(stubRequests.get() === before)
    }
  }

  test("the web UI config announces the enabled proxy") {
    val (status, body) = proxyGet("api/v1/webui/config")
    assert(status === 200)
    assert(body.replace(" ", "").contains("\"engineUIProxyEnabled\":true"))
  }
}

class EngineUIProxyExpectedHostnameSuite extends EngineUIProxySuiteBase {

  override protected lazy val conf: KyuubiConf =
    proxyConf.set(ENGINE_SSL_EXPECTED_HOSTNAME, "other.invalid")

  test("verify engine web UIs against the expected host name instead of the target host") {
    withEngineUI(Some(otherHostKeyStore)) { port =>
      assert(proxyGet(s"engine-ui/https/localhost:$port/jobs/")._1 === 200)
    }
    withEngineUI(Some(goodKeyStore)) { port =>
      assert(proxyGet(s"engine-ui/https/localhost:$port/jobs/")._1 === 502)
    }
  }
}

class EngineUIProxyDisabledSuite extends EngineUIProxySuiteBase {

  override protected lazy val conf: KyuubiConf = KyuubiConf()

  test("the engine UI proxy is disabled by default") {
    withEngineUI(None) { port =>
      val (status, body) = proxyGet(s"engine-ui/localhost:$port/jobs/")
      assert(status === 403)
      assert(body.contains("Engine UI proxy is disabled"))
      assert(stubRequests.get() === 0)
    }
    val (status, body) = proxyGet("api/v1/webui/config")
    assert(status === 200)
    assert(body.replace(" ", "").contains("\"engineUIProxyEnabled\":false"))
  }
}
