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

import java.io.{PrintWriter, StringWriter}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.mockito.Mockito.{verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.api.DeniedRequest.{HostNotAllowed, InvalidAddress, ProxyDisabled}

class EngineUIProxyServletSuite extends KyuubiFunSuite {

  test("rewrite target only for allowlisted engine UI hosts") {
    val servlet = new EngineUIProxyServlet(true, Set("engine-host"), None)

    assert(servlet.rewriteTarget(request("/engine-ui/engine-host:4040")) ===
      "http://engine-host:4040/jobs/")
    assert(servlet.rewriteTarget(request("/engine-ui/engine-host:4040/sql/", "id=1")) ===
      "http://engine-host:4040/sql/?id=1")
    assert(servlet.rewriteTarget(request("/engine-ui/engine-host:4041")) ===
      "http://engine-host:4041/jobs/")
    assert(servlet.rewriteTarget(request("/engine-ui/unknown-host:4040")) === null)
  }

  test("rewrite target denies all proxy traffic when proxy is disabled") {
    val servlet = new EngineUIProxyServlet(false, Set.empty, None)

    assert(servlet.rewriteTarget(request("/engine-ui/engine-host:4040")) === null)
    assert(servlet.rewriteTarget(request("/engine-ui/https/engine-host:4440")) === null)
  }

  test("rewrite target host matching is case-insensitive") {
    val servlet = new EngineUIProxyServlet(true, Set("ENGINE-HOST"), None)

    assert(servlet.rewriteTarget(request("/engine-ui/engine-host:4040")) ===
      "http://engine-host:4040/jobs/")
    assert(servlet.rewriteTarget(request("/engine-ui/OTHER-HOST:4040")) === null)
  }

  test("rewrite target supports wildcard host matching") {
    val servlet = new EngineUIProxyServlet(true, Set("*.example.com", "engine-*.internal"), None)

    assert(servlet.rewriteTarget(request("/engine-ui/spark.example.com:4040")) ===
      "http://spark.example.com:4040/jobs/")
    assert(servlet.rewriteTarget(request("/engine-ui/ENGINE-1.internal:4040")) ===
      "http://ENGINE-1.internal:4040/jobs/")
    assert(servlet.rewriteTarget(request("/engine-ui/example.com:4040")) === null)
    assert(servlet.rewriteTarget(request("/engine-ui/evil-example.com:4040")) === null)
  }

  test("empty hosts deny engine UI targets when proxy is enabled") {
    val servlet = new EngineUIProxyServlet(true, Set.empty, None)

    assert(servlet.rewriteTarget(request("/engine-ui/engine-host:4040")) === null)
  }

  test("denied response explains the required engine UI proxy configuration") {
    val servlet = new EngineUIProxyServlet(false, Set.empty, None)
    val response = mock[HttpServletResponse]
    val body = new StringWriter
    when(response.getWriter).thenReturn(new PrintWriter(body))

    servlet.onProxyRewriteFailed(request("/engine-ui/engine-host:4040/"), response)

    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN)
    verify(response).setContentType("text/html")
    verify(response).setCharacterEncoding("UTF-8")
    assert(body.toString.contains("Target engine"))
    assert(body.toString.contains("engine-host:4040"))
    assert(body.toString.contains("kyuubi.frontend.rest.engine.ui.proxy.enabled"))
    assert(body.toString.contains("kyuubi.frontend.rest.engine.ui.proxy.hosts"))
  }

  test("the reason a request is denied") {
    val disabled = new EngineUIProxyServlet(false, Set("engine-host"), None)
    assert(disabled.deniedRequest("/engine-ui/engine-host:4040/") ===
      ProxyDisabled(Some(TargetAddress("engine-host", 4040, "/"))))
    assert(disabled.deniedRequest("/engine-ui/engine-host") === ProxyDisabled(None))

    val enabled = new EngineUIProxyServlet(true, Set("engine-host"), None)
    assert(enabled.deniedRequest("/engine-ui/https/other-host:4440/jobs/") ===
      HostNotAllowed(TargetAddress("other-host", 4440, "/jobs/", "https")))
    assert(enabled.deniedRequest("/engine-ui/engine-host") === InvalidAddress)
  }

  test("rewrite target denies request smuggling through user info") {
    val servlet = new EngineUIProxyServlet(true, Set("spark-x"), None)

    assert(servlet.rewriteTarget(
      request("/engine-ui/spark-x@100.100.100.200:80/latest/meta-data/")) === null)
    assert(servlet.rewriteTarget(
      request("/engine-ui/https/spark-x@100.100.100.200:443/latest/meta-data/")) === null)
  }

  test("extract target address with URI parser") {
    val servlet = new EngineUIProxyServlet(true, Set("engine-host"), None)

    assert(servlet.extractTargetAddress("/engine-ui/ENGINE-HOST:4040/sql/") ===
      Some(TargetAddress("ENGINE-HOST", 4040, "/sql/")))
    assert(servlet.extractTargetAddress("/engine-ui/engine-host") === None)
    assert(servlet.extractTargetAddress("/api/v1/version") === None)
  }

  test("rewrite target keeps the HTTPS scheme of an engine web UI") {
    val servlet = new EngineUIProxyServlet(true, Set("engine-host", "https"), None)

    assert(servlet.rewriteTarget(request("/engine-ui/https/engine-host:4440")) ===
      "https://engine-host:4440/jobs/")
    assert(servlet.rewriteTarget(request("/engine-ui/https/engine-host:4440/sql/", "id=1")) ===
      "https://engine-host:4440/sql/?id=1")
    assert(servlet.rewriteTarget(request("/engine-ui/https/unknown-host:4440")) === null)
    // a host named https is still reached over plain HTTP
    assert(servlet.rewriteTarget(request("/engine-ui/https:4040/")) === "http://https:4040/jobs/")
  }

  test("the base path of an engine web UI keeps the HTTPS prefix") {
    val servlet = new EngineUIProxyServlet(true, Set("engine-host"), None)

    val https = servlet.extractTargetAddress("/engine-ui/https/engine-host:4440/sql/")
    assert(https === Some(TargetAddress("engine-host", 4440, "/sql/", "https")))
    assert(https.map(_.basePath) === Some("/engine-ui/https/engine-host:4440"))
    assert(servlet.extractTargetAddress("/engine-ui/engine-host:4040/").map(_.basePath) ===
      Some("/engine-ui/engine-host:4040"))
  }

  private def request(uri: String, queryString: String = null): HttpServletRequest = {
    val request = mock[HttpServletRequest]
    when(request.getRequestURL).thenReturn(new StringBuffer(s"http://kyuubi$uri"))
    when(request.getRequestURI).thenReturn(uri)
    when(request.getQueryString).thenReturn(queryString)
    request
  }
}
