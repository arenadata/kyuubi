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

import java.net.{URI, URL}
import java.util.Locale
import java.util.regex.Pattern
import javax.net.ssl.SSLEngine
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.util.Try

import org.apache.commons.lang3.StringUtils
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.proxy.ProxyServlet
import org.eclipse.jetty.util.ssl.SslContextFactory

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.KyuubiRestFrontendService
import org.apache.kyuubi.server.api.DeniedRequest.{HostNotAllowed, InvalidAddress, ProxyDisabled}
import org.apache.kyuubi.util.EngineSslClientContext

/**
 * Proxies the web UIs of engines on the allowed hosts. A web UI served over HTTPS is addressed
 * as /engine-ui/https/host:port, and its certificate is verified like the certificates of the
 * engine listeners: with the engine trust store, against the expected host name when set.
 *
 * @param engineSsl the trust material for web UIs served over HTTPS, the JVM default trust
 *                  store when not given
 */
private[api] class EngineUIProxyServlet(
    proxyEnabled: Boolean,
    allowedHosts: Set[String],
    engineSsl: Option[EngineSslClientContext]) extends ProxyServlet with Logging {

  private val allowedHostPatterns =
    allowedHosts.map(EngineUIProxyServlet.normalizeHost).filter(_.nonEmpty).map(
      EngineUIProxyServlet.wildcardHostPattern)

  override def rewriteTarget(request: HttpServletRequest): String = {
    val requestURL = request.getRequestURL
    val requestURI = request.getRequestURI
    // A null target is handled by onProxyRewriteFailed with an actionable 403 response.
    val targetURL = allowedTarget(requestURI).map { targetAddress =>
      val targetURI = targetAddress.path match {
        // for some reason, the proxy can not handle redirect well, as a workaround,
        // we simulate the Spark UI redirection behavior and forcibly rewrite the
        // empty URI to the Spark Jobs page.
        case "" | "/" => "/jobs/"
        case path => path
      }
      val targetQueryString =
        Option(
          request.getQueryString).filter(StringUtils.isNotEmpty).map(q => s"?$q").getOrElse("")
      new URL(
        targetAddress.scheme,
        targetAddress.host,
        targetAddress.port,
        targetURI + targetQueryString).toString
    }.orNull
    debug(s"rewrite $requestURL => $targetURL")
    targetURL
  }

  override def onProxyRewriteFailed(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    response.setStatus(HttpServletResponse.SC_FORBIDDEN)
    response.setCharacterEncoding("UTF-8")
    response.setContentType("text/html")
    response.getWriter.write(EngineUIProxyDeniedPage.render(deniedRequest(request.getRequestURI)))
  }

  override protected def newHttpClient(): HttpClient = engineSsl match {
    case Some(ssl) =>
      val sslContextFactory = new SslContextFactory.Client() {
        // the host also becomes the TLS server name and the name the certificate is verified
        // against
        override def newSSLEngine(host: String, port: Int): SSLEngine =
          super.newSSLEngine(ssl.expectedHostname.getOrElse(host), port)
      }
      sslContextFactory.setSslContext(ssl.sslContext)
      new HttpClient(sslContextFactory)
    case None => super.newHttpClient()
  }

  override def addXForwardedHeaders(
      clientRequest: HttpServletRequest,
      proxyRequest: Request): Unit = {
    allowedTarget(clientRequest.getRequestURI).foreach { target =>
      // SPARK-24209: Knox uses X-Forwarded-Context to notify the application the base path
      proxyRequest.header("X-Forwarded-Context", target.basePath)
    }
    super.addXForwardedHeaders(clientRequest, proxyRequest)
  }

  private def isAllowedHost(host: String): Boolean =
    proxyEnabled && {
      val normalizedHost = EngineUIProxyServlet.normalizeHost(host)
      allowedHostPatterns.exists(_.matcher(normalizedHost).matches())
    }

  private def allowedTarget(requestURI: String): Option[TargetAddress] =
    extractTargetAddress(requestURI).filter(target => isAllowedHost(target.host))

  /** Why a request is denied, for a request whose target is not rewritten. */
  private[api] def deniedRequest(requestURI: String): DeniedRequest = {
    val target = extractTargetAddress(requestURI)
    if (!proxyEnabled) {
      ProxyDisabled(target)
    } else {
      target.map(HostNotAllowed(_)).getOrElse(InvalidAddress)
    }
  }

  private[api] def extractTargetAddress(requestURI: String): Option[TargetAddress] = {
    val target = requestURI.stripPrefix("/engine-ui/")
    if (target == requestURI) {
      None
    } else {
      val (scheme, address) =
        if (target.startsWith("https/")) ("https", target.stripPrefix("https/"))
        else ("http", target)
      Try(new URI(s"$scheme://$address")).toOption.flatMap { uri =>
        for {
          host <- Option(uri.getHost)
          port <- Option(uri.getPort).filter(_ >= 0)
        } yield TargetAddress(
          host,
          port,
          Option(uri.getRawPath).filter(_.nonEmpty).getOrElse("/"),
          scheme)
      }
    }
  }

}

private[api] case class TargetAddress(
    host: String,
    port: Int,
    path: String,
    scheme: String = "http") {

  /** The path the web UI is served under by the proxy. */
  def basePath: String =
    if (scheme == "https") s"/engine-ui/https/$host:$port" else s"/engine-ui/$host:$port"
}

/** Why the engine UI proxy denies a request. */
sealed private[api] trait DeniedRequest {

  /** The engine web UI the request is for, when the request has a valid engine address. */
  def target: Option[TargetAddress]
}

private[api] object DeniedRequest {

  case class ProxyDisabled(target: Option[TargetAddress]) extends DeniedRequest

  case class HostNotAllowed(address: TargetAddress) extends DeniedRequest {
    override def target: Option[TargetAddress] = Some(address)
  }

  case object InvalidAddress extends DeniedRequest {
    override def target: Option[TargetAddress] = None
  }
}

private[api] object EngineUIProxyServlet {

  def apply(fe: KyuubiRestFrontendService): EngineUIProxyServlet = {
    val proxyEnabled = fe.getConf.get(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_ENABLED)
    new EngineUIProxyServlet(
      proxyEnabled,
      fe.getConf.get(KyuubiConf.FRONTEND_REST_ENGINE_UI_PROXY_HOSTS).toSet,
      // only loaded for an enabled proxy, an invalid trust store then fails the startup
      if (proxyEnabled) Some(fe.sessionManager.engineSslClientContext) else None)
  }

  private[api] def normalizeHost(host: String): String = host.trim.toLowerCase(Locale.ROOT)

  private[api] def wildcardHostPattern(hostPattern: String): Pattern = {
    Pattern.compile(hostPattern.map {
      case '*' => ".*"
      case c => Pattern.quote(c.toString)
    }.mkString("^", "", "$"))
  }
}
