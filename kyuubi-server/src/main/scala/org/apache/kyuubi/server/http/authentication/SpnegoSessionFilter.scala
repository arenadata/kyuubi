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

package org.apache.kyuubi.server.http.authentication

import javax.security.auth.Subject
import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.http.util.HttpAuthUtils.{NEGOTIATE, WWW_AUTHENTICATE_HEADER}

class SpnegoSessionFilter(conf: KyuubiConf) extends Filter with Logging {

  private var serverSubject = new Subject()
  private var keytab: String = _
  private val sessionTimeout = (conf.get(KyuubiConf.SESSION_IDLE_TIMEOUT) / 1000).toInt
  private val atrSpnegoIsAuth = "spnego.authenticated"
  private val atrSpnegoPrincipal = "spnego.principal"
  private val handler = new KerberosAuthenticationHandler()

  override def init(cfg: FilterConfig): Unit = {
    handler.init(conf)
  }

  override def destroy(): Unit = {
    keytab = null
    serverSubject = null
    handler.destroy()
  }

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain): Unit = {

    val httpReq = request.asInstanceOf[HttpServletRequest]
    val httpResp = response.asInstanceOf[HttpServletResponse]
    val session = httpReq.getSession(false)

    // check current session
    if (session != null
      && session.getAttribute(atrSpnegoIsAuth) == java.lang.Boolean.TRUE) {
      chain.doFilter(request, response)
      return
    }

    // check auth header
    httpReq.getHeader("Authorization") match {
      case null =>
        httpResp.setHeader(WWW_AUTHENTICATE_HEADER, NEGOTIATE) // it must before sendError
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "No SPNEGO token")

      case authHeader if authHeader.startsWith("Negotiate ") =>
        try {
          val principal = handler.authenticate(httpReq, httpResp) // auth SPNEGO

          if (principal != null) {
            // create new session
            val newSession = httpReq.getSession(true)
            newSession.setAttribute(atrSpnegoIsAuth, true)
            newSession.setAttribute(atrSpnegoPrincipal, principal)
            newSession.setMaxInactiveInterval(sessionTimeout)

            chain.doFilter(request, response)
          } else {
            httpResp.sendError(HttpServletResponse.SC_FORBIDDEN, "Authentication failed")
          }
        } catch {
          case e: Exception =>
            error("Exception ", e)
            httpResp.sendError(
              HttpServletResponse.SC_UNAUTHORIZED,
              s"Invalid SPNEGO token ${e.getMessage}")
        }

      case _ =>
        httpResp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid Authorization header")
    }
  }
}
