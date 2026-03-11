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

import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse, HttpSession}

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class SpnegoSessionFilterSuite extends KyuubiFunSuite with MockitoSugar {

  private val sessionTimeout = 3600
  private val conf = new KyuubiConf()
    .set(KyuubiConf.SESSION_IDLE_TIMEOUT, sessionTimeout * 1000L)

  private def createFilter(): SpnegoSessionFilter = {
    val filter = new SpnegoSessionFilter(conf)
    val filterConfig = mock[FilterConfig]
    filter.init(filterConfig)
    filter
  }

  test("doFilter should allow request with existing authenticated session") {
    // Setup
    val filter = createFilter()
    val request = mock[HttpServletRequest]
    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]
    val session = mock[HttpSession]

    when(request.getSession(false)).thenReturn(session)
    when(session.getAttribute("spnego.authenticated")).thenReturn(java.lang.Boolean.TRUE)

    // Execute
    filter.doFilter(request, response, chain)

    // Verify
    verify(chain).doFilter(request, response)
    verify(response, never()).sendError(anyInt(), anyString())
    verify(request, never()).getHeader("Authorization")
  }

  test("doFilter should return UNAUTHORIZED when no Authorization header") {
    // Setup
    val filter = createFilter()
    val request = mock[HttpServletRequest]
    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]

    when(request.getSession(false)).thenReturn(null)
    when(request.getHeader("Authorization")).thenReturn(null)

    // Execute
    filter.doFilter(request, response, chain)

    // Verify
    verify(response).setHeader("WWW-Authenticate", "Negotiate")
    verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "No SPNEGO token")
    verify(chain, never()).doFilter(any(), any())
  }

  test("doFilter should handle successful SPNEGO authentication and create session") {
    // Setup
    val filter = createFilter()
    val request = mock[HttpServletRequest]
    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]
    val session = mock[HttpSession]
    val principal = "testuser@REALM"

    // Mock handler to return principal
    val handlerField = classOf[SpnegoSessionFilter].getDeclaredField("handler")
    handlerField.setAccessible(true)
    val mockHandler = mock[KerberosAuthenticationHandler]
    handlerField.set(filter, mockHandler)

    when(request.getSession(false)).thenReturn(null)
    when(request.getHeader("Authorization")).thenReturn("Negotiate dGVzdHRva2Vu")
    when(mockHandler.authenticate(request, response)).thenReturn(principal)
    when(request.getSession(true)).thenReturn(session)

    // Execute
    filter.doFilter(request, response, chain)

    // Verify
    verify(mockHandler).authenticate(request, response)
    verify(session).setAttribute("spnego.authenticated", true)
    verify(session).setAttribute("spnego.principal", principal)
    verify(session).setMaxInactiveInterval(sessionTimeout)
    verify(chain).doFilter(request, response)
  }

  test("doFilter should return FORBIDDEN when authentication returns null principal") {
    // Setup
    val filter = createFilter()
    val request = mock[HttpServletRequest]
    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]

    // Mock handler to return null
    val handlerField = classOf[SpnegoSessionFilter].getDeclaredField("handler")
    handlerField.setAccessible(true)
    val mockHandler = mock[KerberosAuthenticationHandler]
    handlerField.set(filter, mockHandler)

    when(request.getSession(false)).thenReturn(null)
    when(request.getHeader("Authorization")).thenReturn("Negotiate dGVzdHRva2Vu")
    when(mockHandler.authenticate(request, response)).thenReturn(null)

    // Execute
    filter.doFilter(request, response, chain)

    // Verify
    verify(mockHandler).authenticate(request, response)
    verify(response).sendError(HttpServletResponse.SC_FORBIDDEN, "Authentication failed")
    verify(chain, never()).doFilter(any(), any())
  }

  test("doFilter should handle existing session but not authenticated") {
    // Setup
    val filter = createFilter()
    val request = mock[HttpServletRequest]
    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]
    val session = mock[HttpSession]

    when(request.getSession(false)).thenReturn(session)
    when(session.getAttribute("spnego.authenticated")).thenReturn(null)
    when(request.getHeader("Authorization")).thenReturn(null)

    // Execute
    filter.doFilter(request, response, chain)

    // Verify - should treat as not authenticated and proceed with SPNEGO auth
    verify(response).setHeader("WWW-Authenticate", "Negotiate")
    verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "No SPNEGO token")
    verify(chain, never()).doFilter(any(), any())
  }

  test("init and destroy should work correctly") {
    val filter = new SpnegoSessionFilter(conf)
    val filterConfig = mock[FilterConfig]

    // Test init
    filter.init(filterConfig)

    // Verify handler was initialized
    val handlerField = classOf[SpnegoSessionFilter].getDeclaredField("handler")
    handlerField.setAccessible(true)
    val handler = handlerField.get(filter).asInstanceOf[KerberosAuthenticationHandler]
    assert(handler != null)

    // Test destroy
    filter.destroy()

    // Verify state after destroy
    val keytabField = classOf[SpnegoSessionFilter].getDeclaredField("keytab")
    keytabField.setAccessible(true)
    assert(keytabField.get(filter) == null)

    val subjectField = classOf[SpnegoSessionFilter].getDeclaredField("serverSubject")
    subjectField.setAccessible(true)
    assert(subjectField.get(filter) == null)
  }

  test("doFilter should create session with correct timeout value") {
    // Setup with custom timeout
    val customTimeout = 7200
    val customConf = new KyuubiConf()
      .set(KyuubiConf.SESSION_IDLE_TIMEOUT, customTimeout * 1000L)
    val filter = new SpnegoSessionFilter(customConf)
    val filterConfig = mock[FilterConfig]
    filter.init(filterConfig)

    val request = mock[HttpServletRequest]
    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]
    val session = mock[HttpSession]
    val principal = "testuser@REALM"

    // Mock handler
    val handlerField = classOf[SpnegoSessionFilter].getDeclaredField("handler")
    handlerField.setAccessible(true)
    val mockHandler = mock[KerberosAuthenticationHandler]
    handlerField.set(filter, mockHandler)

    when(request.getSession(false)).thenReturn(null)
    when(request.getHeader("Authorization")).thenReturn("Negotiate dGVzdHRva2Vu")
    when(mockHandler.authenticate(request, response)).thenReturn(principal)
    when(request.getSession(true)).thenReturn(session)

    // Execute
    filter.doFilter(request, response, chain)

    // Verify custom timeout was set
    verify(session).setMaxInactiveInterval(customTimeout)
  }

  test("doFilter should handle different Negotiate header formats") {
    val filter = createFilter()
    val request = mock[HttpServletRequest]
    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]

    // Test with different case and spacing
    val testHeaders = Seq(
      "Negotiate dGVzdHRva2Vu",
      "NEGOTIATE dGVzdHRva2Vu",
      "negotiate dGVzdHRva2Vu",
      "Negotiate  dGVzdHRva2Vu" // with extra space
    )

    testHeaders.foreach { header =>
      reset(request, response, chain)

      val handlerField = classOf[SpnegoSessionFilter].getDeclaredField("handler")
      handlerField.setAccessible(true)
      val mockHandler = mock[KerberosAuthenticationHandler]
      handlerField.set(filter, mockHandler)

      when(request.getSession(false)).thenReturn(null)
      when(request.getHeader("Authorization")).thenReturn(header)
      when(mockHandler.authenticate(request, response)).thenReturn("testuser")

      filter.doFilter(request, response, chain)

      verify(mockHandler).authenticate(request, response)
    }
  }
}
