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

package org.apache.kyuubi.server.grpc

import java.util.Base64

import io.grpc.{Metadata, ServerCall, ServerCallHandler, Status}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatestplus.mockito.MockitoSugar

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.AUTHENTICATION_METHOD

class SparkConnectAuthInterceptorSuite extends KyuubiFunSuite with MockitoSugar {

  private val USER_KEY = SparkConnectAuthInterceptor.USER_KEY

  private def makeHeaders(
      authValue: Option[String] = None,
      xUser: Option[String] = None): Metadata = {
    val headers = new Metadata()
    authValue.foreach { v =>
      headers.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), v)
    }
    xUser.foreach { u =>
      headers.put(Metadata.Key.of("x-user-name", Metadata.ASCII_STRING_MARSHALLER), u)
    }
    headers
  }

  test("NOSASL: x-user-name header sets USER_KEY") {
    val conf = KyuubiConf().set(AUTHENTICATION_METHOD, Seq("NOSASL"))
    val interceptor = new SparkConnectAuthInterceptor(conf)

    val call = mock[ServerCall[Array[Byte], Array[Byte]]]
    val handler = mock[ServerCallHandler[Array[Byte], Array[Byte]]]
    var capturedUser: String = null
    when(handler.startCall(any(), any())).thenAnswer((_: InvocationOnMock) => {
      capturedUser = USER_KEY.get()
      null.asInstanceOf[ServerCall.Listener[Array[Byte]]]
    })

    interceptor.interceptCall(call, makeHeaders(xUser = Some("vit")), handler)

    assert(capturedUser === "vit")
  }

  test("NOSASL: missing x-user-name falls back to system user") {
    val conf = KyuubiConf().set(AUTHENTICATION_METHOD, Seq("NOSASL"))
    val interceptor = new SparkConnectAuthInterceptor(conf)

    val call = mock[ServerCall[Array[Byte], Array[Byte]]]
    val handler = mock[ServerCallHandler[Array[Byte], Array[Byte]]]
    var capturedUser: String = null
    when(handler.startCall(any(), any())).thenAnswer((_: InvocationOnMock) => {
      capturedUser = USER_KEY.get()
      null.asInstanceOf[ServerCall.Listener[Array[Byte]]]
    })

    interceptor.interceptCall(call, makeHeaders(), handler)

    assert(capturedUser === System.getProperty("user.name", "anonymous"))
  }

  test("missing Authorization header with non-NOSASL auth returns UNAUTHENTICATED") {
    val conf = KyuubiConf() // default NONE, saslDisabled = false
    val interceptor = new SparkConnectAuthInterceptor(conf)

    val call = mock[ServerCall[Array[Byte], Array[Byte]]]
    val handler = mock[ServerCallHandler[Array[Byte], Array[Byte]]]
    val statusCaptor = ArgumentCaptor.forClass(classOf[Status])

    interceptor.interceptCall(call, makeHeaders(), handler)

    verify(call).close(statusCaptor.capture(), any())
    assert(statusCaptor.getValue.getCode === Status.Code.UNAUTHENTICATED)
  }

  test("Negotiate header returns UNAUTHENTICATED, SPNEGO is unsupported here") {
    val conf = KyuubiConf() // SERVER_SPNEGO_KEYTAB/PRINCIPAL not set -> kerberosValidator = None
    val interceptor = new SparkConnectAuthInterceptor(conf)

    val call = mock[ServerCall[Array[Byte], Array[Byte]]]
    val handler = mock[ServerCallHandler[Array[Byte], Array[Byte]]]
    val statusCaptor = ArgumentCaptor.forClass(classOf[Status])

    val someToken = Base64.getEncoder.encodeToString("sometoken".getBytes)
    interceptor.interceptCall(call, makeHeaders(authValue = Some(s"Negotiate $someToken")), handler)

    verify(call).close(statusCaptor.capture(), any())
    assert(statusCaptor.getValue.getCode === Status.Code.UNAUTHENTICATED)
    assert(statusCaptor.getValue.getDescription.contains("Unsupported Authorization scheme"))
  }

  test("Bearer: no tokenStore configured returns UNAUTHENTICATED") {
    val conf = KyuubiConf()
    val interceptor = new SparkConnectAuthInterceptor(conf, tokenStore = None)

    val call = mock[ServerCall[Array[Byte], Array[Byte]]]
    val handler = mock[ServerCallHandler[Array[Byte], Array[Byte]]]
    val statusCaptor = ArgumentCaptor.forClass(classOf[Status])

    interceptor.interceptCall(call, makeHeaders(authValue = Some("Bearer some-uuid")), handler)

    verify(call).close(statusCaptor.capture(), any())
    assert(statusCaptor.getValue.getCode === Status.Code.UNAUTHENTICATED)
  }

  test("Bearer: valid token sets USER_KEY") {
    val conf = KyuubiConf()
    val store = new SparkConnectTokenStore(ttlMs = 60000L)
    try {
      val (token, _) = store.create("john")
      val interceptor = new SparkConnectAuthInterceptor(conf, tokenStore = Some(store))

      val call = mock[ServerCall[Array[Byte], Array[Byte]]]
      val handler = mock[ServerCallHandler[Array[Byte], Array[Byte]]]
      var capturedUser: String = null
      when(handler.startCall(any(), any())).thenAnswer((_: InvocationOnMock) => {
        capturedUser = USER_KEY.get()
        null.asInstanceOf[ServerCall.Listener[Array[Byte]]]
      })

      interceptor.interceptCall(call, makeHeaders(authValue = Some(s"Bearer $token")), handler)

      assert(capturedUser === "john")
    } finally {
      store.stop()
    }
  }

  test("Bearer: successful request renews token TTL") {
    val conf = KyuubiConf()
    val realStore = new SparkConnectTokenStore(ttlMs = 60000L)
    val store = spy(realStore)
    try {
      val (token, _) = store.create("john")
      val interceptor = new SparkConnectAuthInterceptor(conf, tokenStore = Some(store))

      val call = mock[ServerCall[Array[Byte], Array[Byte]]]
      val handler = mock[ServerCallHandler[Array[Byte], Array[Byte]]]
      var capturedUser: String = null
      when(handler.startCall(any(), any())).thenAnswer((_: InvocationOnMock) => {
        capturedUser = USER_KEY.get()
        null.asInstanceOf[ServerCall.Listener[Array[Byte]]]
      })

      interceptor.interceptCall(call, makeHeaders(authValue = Some(s"Bearer $token")), handler)

      assert(capturedUser === "john")
      verify(store).renew(token)
    } finally {
      realStore.stop()
    }
  }

  test("KyuubiToken: expired or unknown token returns UNAUTHENTICATED") {
    val conf = KyuubiConf()
    val store = new SparkConnectTokenStore(ttlMs = -1L) // already expired on creation
    try {
      val (token, _) = store.create("john")
      val interceptor = new SparkConnectAuthInterceptor(conf, tokenStore = Some(store))

      val call = mock[ServerCall[Array[Byte], Array[Byte]]]
      val handler = mock[ServerCallHandler[Array[Byte], Array[Byte]]]
      val statusCaptor = ArgumentCaptor.forClass(classOf[Status])

      interceptor.interceptCall(call, makeHeaders(authValue = Some(s"Bearer $token")), handler)

      verify(call).close(statusCaptor.capture(), any())
      assert(statusCaptor.getValue.getCode === Status.Code.UNAUTHENTICATED)
      assert(statusCaptor.getValue.getDescription.contains("Invalid or expired token"))
    } finally {
      store.stop()
    }
  }

  test("unknown Authorization scheme returns UNAUTHENTICATED") {
    val conf = KyuubiConf()
    val interceptor = new SparkConnectAuthInterceptor(conf)

    val call = mock[ServerCall[Array[Byte], Array[Byte]]]
    val handler = mock[ServerCallHandler[Array[Byte], Array[Byte]]]
    val statusCaptor = ArgumentCaptor.forClass(classOf[Status])

    interceptor.interceptCall(call, makeHeaders(authValue = Some("Foo bar")), handler)

    verify(call).close(statusCaptor.capture(), any())
    assert(statusCaptor.getValue.getCode === Status.Code.UNAUTHENTICATED)
    assert(statusCaptor.getValue.getDescription.contains("Unsupported Authorization scheme"))
  }
}
