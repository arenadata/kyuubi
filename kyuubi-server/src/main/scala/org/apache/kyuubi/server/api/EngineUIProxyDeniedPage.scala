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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.api.DeniedRequest.{HostNotAllowed, InvalidAddress, ProxyDisabled}
import org.apache.kyuubi.server.api.HtmlTemplate.text

/** The page the engine UI proxy answers a denied request with. */
private[api] object EngineUIProxyDeniedPage {

  private val page = HtmlTemplate("engine-ui-proxy/denied.html")
  private val targetEngine = HtmlTemplate("engine-ui-proxy/target.html")
  private val proxyDisabled = HtmlTemplate("engine-ui-proxy/proxy-disabled.html")
  private val proxyDisabledNoTarget = HtmlTemplate("engine-ui-proxy/proxy-disabled-no-target.html")
  private val hostNotAllowed = HtmlTemplate("engine-ui-proxy/host-not-allowed.html")
  private val invalidAddress = HtmlTemplate("engine-ui-proxy/invalid-address.html")

  private val enabledKey =
    "enabledKey" -> text(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED.key)
  private val hostsKey = "hostsKey" -> text(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_HOSTS.key)

  def render(denied: DeniedRequest): String = {
    val (title, message) = denied match {
      case ProxyDisabled(Some(target)) =>
        (
          "Engine UI proxy is disabled",
          proxyDisabled.render(enabledKey, "host" -> text(target.host), hostsKey))
      case ProxyDisabled(None) =>
        ("Engine UI proxy is disabled", proxyDisabledNoTarget.render(enabledKey, hostsKey))
      case HostNotAllowed(target) =>
        (
          "Engine UI host is not allowed",
          hostNotAllowed.render("host" -> text(target.host), hostsKey))
      case InvalidAddress =>
        ("Invalid Engine UI address", invalidAddress.render())
    }
    val target = denied.target.map { address =>
      targetEngine.render("host" -> text(address.host), "port" -> text(address.port.toString))
    }
    page.render(
      "title" -> text(title),
      "target" -> target.getOrElse(text("")),
      "message" -> message).markup
  }
}
