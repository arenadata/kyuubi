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

import java.io.{File, IOException}
import java.security.{PrivilegedActionException, PrivilegedExceptionAction}
import java.util.Base64
import javax.security.auth.Subject
import javax.security.auth.kerberos.{KerberosPrincipal, KeyTab}
import javax.security.sasl.AuthenticationException
import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.hadoop.security.authentication.util.KerberosName
import org.apache.hadoop.security.authentication.util.KerberosUtil._
import org.ietf.jgss.{GSSContext, GSSCredential, GSSManager, Oid}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.http.authentication.AuthSchemes.AuthScheme
import org.apache.kyuubi.server.http.util.HttpAuthUtils.{AUTHORIZATION_HEADER, NEGOTIATE, WWW_AUTHENTICATE_HEADER}

class SpnegoSessionFilter(conf: KyuubiConf) extends Filter with Logging {

  private var gssManager: GSSManager = _
  private var serverSubject = new Subject()
  private var keytab: String = _
  private var principal: String = _
  private val sessionTimeout = conf.get(KyuubiConf.SESSION_IDLE_TIMEOUT).toInt
  private val atrSpnegoIsAuth = "spnego.authenticated"
  private val atrSpnegoPrincipal = "spnego.principal"
  private val authScheme: AuthScheme = AuthSchemes.NEGOTIATE

  private def authenticationSupported: Boolean = {
    keytab.nonEmpty && principal.nonEmpty
  }

  override def init(cfg: FilterConfig): Unit = {
    keytab = conf.get(KyuubiConf.SERVER_SPNEGO_KEYTAB).getOrElse("")
    principal = conf.get(KyuubiConf.SERVER_SPNEGO_PRINCIPAL).getOrElse("")
    if (authenticationSupported) {
      val keytabFile = new File(keytab)
      if (!keytabFile.exists()) {
        throw new ServletException(s"Keytab[$keytab] does not exists")
      }
      if (!principal.startsWith("HTTP/")) {
        throw new ServletException(s"SPNEGO principal[$principal] does not start with HTTP/")
      }

      info(s"Using keytab $keytab, for principal $principal")
      serverSubject.getPrivateCredentials().add(KeyTab.getInstance(keytabFile))
      serverSubject.getPrincipals.add(new KerberosPrincipal(principal))

      // TODO: support to config kerberos.name.rules and kerberos.rule.mechanism
      // set default rules if no rules set, otherwise it will throw exception
      // when parse the kerberos name
      if (!KerberosName.hasRulesBeenSet) {
        KerberosName.setRules("DEFAULT")
      }

      try {
        gssManager = Subject.doAs(
          serverSubject,
          new PrivilegedExceptionAction[GSSManager] {
            override def run(): GSSManager = {
              GSSManager.getInstance()
            }
          })
      } catch {
        case e: PrivilegedActionException => throw e.getException
        case e: Exception => throw new ServletException(e)
      }
    }
  }

  override def destroy(): Unit = {
    keytab = null
    serverSubject = null
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
        httpResp.setHeader(WWW_AUTHENTICATE_HEADER, "Negotiate") // it must before sendError
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED, "No SPNEGO token")

      case authHeader if authHeader.startsWith("Negotiate ") =>
        try {
          val principal = authenticate(httpReq, httpResp) // auth SPNEGO

          if (principal != null) {
            // Создаем новую сессию
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

  def getAuthorization(request: HttpServletRequest): String = {
    val authHeader = request.getHeader(AUTHORIZATION_HEADER)
    // each http request must have an Authorization header
    if (authHeader == null || authHeader.isEmpty) {
      throw new AuthenticationException("Authorization header received from the client is empty.")
    }

    var authorization = authHeader.substring(authScheme.toString.length).trim
    // For thrift http spnego authorization, its format is 'NEGOTIATE : $token', see HIVE-26353
    if (authorization.startsWith(":")) {
      authorization = authorization.stripPrefix(":").trim
    }
    // Authorization header must have a payload
    if (authorization == null || authorization.isEmpty) {
      throw new AuthenticationException(
        "Authorization header received from the client does not contain any data.")
    }
    authorization
  }

  private def authenticate(
      request: HttpServletRequest,
      response: HttpServletResponse): String = {
    var authUser: String = null

    val authorization = getAuthorization(request)
    val clientToken = Base64.getDecoder.decode(authorization)
    try {
      debug("step by step spnego ui")
      val serverPrincipal = getTokenServerName(clientToken)
      debug("step 2 getTokenServerName")
      if (!serverPrincipal.startsWith("HTTP/")) {
        throw new IllegalArgumentException(
          s"Invalid server principal $serverPrincipal decoded from client request")
      }

      debug("step 3 startsWith")
      authUser = Subject.doAs(
        serverSubject,
        new PrivilegedExceptionAction[String] {
          override def run(): String = {
            runWithPrincipal(serverPrincipal, clientToken, response)
          }
        })

      debug("step 4 doAs")
    } catch {
      case ex: PrivilegedActionException =>
        ex.getException match {
          case ioe: IOException =>
            throw ioe
          case e: Exception => throw new AuthenticationException("SPNEGO authentication failed", e)
        }

      case e: Exception => throw new AuthenticationException("SPNEGO authentication failed", e)
    }
    authUser
  }

  private def runWithPrincipal(
      serverPrincipal: String,
      clientToken: Array[Byte],
      response: HttpServletResponse): String = {
    var gssContext: GSSContext = null
    var gssCreds: GSSCredential = null
    var authUser: String = null
    try {
      debug(s"SPNEGO initialized with server principal $serverPrincipal")
      gssCreds = gssManager.createCredential(
        gssManager.createName(serverPrincipal, NT_GSS_KRB5_PRINCIPAL_OID),
        GSSCredential.INDEFINITE_LIFETIME,
        Array[Oid](GSS_SPNEGO_MECH_OID, GSS_KRB5_MECH_OID),
        GSSCredential.ACCEPT_ONLY)
      gssContext = gssManager.createContext(gssCreds)
      val serverToken = gssContext.acceptSecContext(clientToken, 0, clientToken.length)
      if (serverToken != null && serverToken.nonEmpty) {
        val authenticate = Base64.getEncoder.encodeToString(serverToken)
        response.setHeader(WWW_AUTHENTICATE_HEADER, s"$NEGOTIATE $authenticate")
      }
      if (!gssContext.isEstablished) {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
        debug("SPNEGO in progress")
      } else {
        val clientPrincipal = gssContext.getSrcName.toString
        val kerberosName = new KerberosName(clientPrincipal)
        val userName = kerberosName.getShortName
        authUser = userName
        response.setStatus(HttpServletResponse.SC_OK)
        debug(s"SPNEGO completed for client principal $clientPrincipal")
      }
    } finally {
      if (gssContext != null) {
        gssContext.dispose()
      }
      if (gssCreds != null) {
        gssCreds.dispose()
      }
    }
    authUser
  }
}
