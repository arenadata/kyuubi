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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_REST_ENGINE_UI_PROXY_ENABLED, FRONTEND_REST_ENGINE_UI_PROXY_HOSTS}
import org.apache.kyuubi.server.api.DeniedRequest.{HostNotAllowed, InvalidAddress, ProxyDisabled}

class EngineUIProxyDeniedPageSuite extends KyuubiFunSuite {

  private val target = TargetAddress("engine-host", 4040, "/jobs/")
  private val enabledKey = FRONTEND_REST_ENGINE_UI_PROXY_ENABLED.key
  private val hostsKey = FRONTEND_REST_ENGINE_UI_PROXY_HOSTS.key
  private val targetBlock = "<div class=\"target\"> <span>Target engine</span> " +
    "<code>engine-host:4040</code> </div>"

  test("the page for a disabled proxy") {
    val page = render(ProxyDisabled(Some(target)))
    assertTitle(page, "Engine UI proxy is disabled")
    assert(page.contains(targetBlock))
    assert(page.contains(s"<p>Ask an administrator to set <code>$enabledKey</code> to " +
      s"<code>true</code>, then add <code>engine-host</code> to <code>$hostsKey</code>.</p>"))
  }

  test("the page for a disabled proxy and an invalid engine address") {
    val page = render(ProxyDisabled(None))
    assertTitle(page, "Engine UI proxy is disabled")
    assert(!page.contains("class=\"target\""))
    assert(page.contains(s"<p>Ask an administrator to set <code>$enabledKey</code> to " +
      s"<code>true</code>, then add the target host to <code>$hostsKey</code>.</p>"))
  }

  test("the page for a host that is not allowed") {
    val page = render(HostNotAllowed(target))
    assertTitle(page, "Engine UI host is not allowed")
    assert(page.contains(targetBlock))
    assert(page.contains(
      s"<p>Ask an administrator to add <code>engine-host</code> to <code>$hostsKey</code>.</p>"))
    assert(!page.contains(enabledKey))
  }

  test("the page for an invalid engine address") {
    val page = render(InvalidAddress)
    assertTitle(page, "Invalid Engine UI address")
    assert(!page.contains("class=\"target\""))
    assert(page.contains("<p>The URL must include an engine host and port, for example " +
      "<code>/engine-ui/spark.example.com:4040/</code>.</p>"))
  }

  test("escape the engine host") {
    val page = render(HostNotAllowed(TargetAddress("<b>engine</b>", 4040, "/")))
    assert(!page.contains("<b>engine"))
    assert(page.contains("<code>&lt;b&gt;engine&lt;/b&gt;:4040</code>"))
    assert(page.contains("add <code>&lt;b&gt;engine&lt;/b&gt;</code> to"))
  }

  test("every page is a complete HTML document") {
    Seq(ProxyDisabled(Some(target)), ProxyDisabled(None), HostNotAllowed(target), InvalidAddress)
      .foreach { denied =>
        val page = EngineUIProxyDeniedPage.render(denied)
        assert(page.startsWith("<!DOCTYPE html>"))
        assert(page.endsWith("</html>"))
        assert(page.contains("<div class=\"status\">403 / ACCESS DENIED</div>"))
        assert(!page.contains("{{"))
        assert(!page.contains("Licensed to the Apache Software Foundation"))
      }
  }

  /** @return the page with the white space collapsed, as a browser shows it */
  private def render(denied: DeniedRequest): String =
    EngineUIProxyDeniedPage.render(denied).replaceAll("\\s+", " ")

  private def assertTitle(page: String, title: String): Unit = {
    assert(page.contains(s"<title>403 - $title</title>"))
    assert(page.contains(s"<h1>$title</h1>"))
  }
}
