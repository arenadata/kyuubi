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

import io.grpc.{Context, Status}
import io.grpc.stub.StreamObserver
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.grpc.proto._

class SparkConnectAuthServiceImplSuite extends KyuubiFunSuite with MockitoSugar {

  private val TTL_MS = 60000L

  // Run block with AUTH_HEADER_KEY set in gRPC Context
  private def withAuthHeader[T](header: String)(block: => T): T = {
    val ctx = Context.current().withValue(SparkConnectRawHeaderContext.AUTH_HEADER_KEY, header)
    ctx.call(() => block)
  }

  // getToken

  test("getToken: missing Authorization header returns UNAUTHENTICATED") {
    val store = new InMemoryTokenStore(TTL_MS)
    try {
      val service = new SparkConnectAuthServiceImpl(handlers = Seq.empty, store)
      val observer = mock[StreamObserver[GetTokenResponse]]
      val errorCaptor = ArgumentCaptor.forClass(classOf[Throwable])

      // no withAuthHeader -> AUTH_HEADER_KEY.get() returns null
      service.getToken(GetTokenRequest.getDefaultInstance, observer)

      verify(observer).onError(errorCaptor.capture())
      val status = Status.fromThrowable(errorCaptor.getValue)
      assert(status.getCode === Status.Code.UNAUTHENTICATED)
      assert(status.getDescription.contains("Missing Authorization header"))
    } finally {
      store.stop()
    }
  }

  test("getToken: handler authenticates successfully returns token and expiry") {
    val store = new InMemoryTokenStore(TTL_MS)
    try {
      val handler = mock[SparkConnectCredentialHandler]
      when(handler.authenticate(any())).thenReturn(Some("john"))
      val service = new SparkConnectAuthServiceImpl(handlers = Seq(handler), store)
      val observer = mock[StreamObserver[GetTokenResponse]]
      val responseCaptor = ArgumentCaptor.forClass(classOf[GetTokenResponse])

      withAuthHeader("Basic dXNlcjpwYXNz") {
        service.getToken(GetTokenRequest.getDefaultInstance, observer)
      }

      verify(observer).onNext(responseCaptor.capture())
      verify(observer).onCompleted()
      val response = responseCaptor.getValue
      assert(response.getToken.nonEmpty)
      assert(response.getExpiresAtMs > System.currentTimeMillis())
      assert(store.getUser(response.getToken) === Some("john"))
    } finally {
      store.stop()
    }
  }

  test("getToken: handler throws returns UNAUTHENTICATED with cause message") {
    val store = new InMemoryTokenStore(TTL_MS)
    try {
      val handler = mock[SparkConnectCredentialHandler]
      when(handler.authenticate(any())).thenThrow(new RuntimeException("bad credentials"))
      val service = new SparkConnectAuthServiceImpl(handlers = Seq(handler), store)
      val observer = mock[StreamObserver[GetTokenResponse]]
      val errorCaptor = ArgumentCaptor.forClass(classOf[Throwable])

      withAuthHeader("Basic dXNlcjpwYXNz") {
        service.getToken(GetTokenRequest.getDefaultInstance, observer)
      }

      verify(observer).onError(errorCaptor.capture())
      val status = Status.fromThrowable(errorCaptor.getValue)
      assert(status.getCode === Status.Code.UNAUTHENTICATED)
      assert(status.getDescription.contains("bad credentials"))
    } finally {
      store.stop()
    }
  }

  test("getToken: no handler matches scheme returns UNAUTHENTICATED") {
    val store = new InMemoryTokenStore(TTL_MS)
    try {
      val handler = mock[SparkConnectCredentialHandler]
      when(handler.authenticate(any())).thenReturn(None)
      val service = new SparkConnectAuthServiceImpl(handlers = Seq(handler), store)
      val observer = mock[StreamObserver[GetTokenResponse]]
      val errorCaptor = ArgumentCaptor.forClass(classOf[Throwable])

      withAuthHeader("Unknownheader value") {
        service.getToken(GetTokenRequest.getDefaultInstance, observer)
      }

      verify(observer).onError(errorCaptor.capture())
      val status = Status.fromThrowable(errorCaptor.getValue)
      assert(status.getCode === Status.Code.UNAUTHENTICATED)
      assert(status.getDescription.contains("Unsupported Authorization scheme"))
    } finally {
      store.stop()
    }
  }

  // renewToken

  test("renewToken: valid token returns new expiry") {
    val store = new InMemoryTokenStore(TTL_MS)
    try {
      val (token, originalExpiry) = store.create("john")
      Thread.sleep(5)
      val service = new SparkConnectAuthServiceImpl(handlers = Seq.empty, store)
      val observer = mock[StreamObserver[RenewTokenResponse]]
      val responseCaptor = ArgumentCaptor.forClass(classOf[RenewTokenResponse])

      service.renewToken(RenewTokenRequest.newBuilder().setToken(token).build(), observer)

      verify(observer).onNext(responseCaptor.capture())
      verify(observer).onCompleted()
      assert(responseCaptor.getValue.getExpiresAtMs >= originalExpiry)
    } finally {
      store.stop()
    }
  }

  test("renewToken: expired or unknown token returns UNAUTHENTICATED") {
    val store = new InMemoryTokenStore(TTL_MS)
    try {
      val service = new SparkConnectAuthServiceImpl(handlers = Seq.empty, store)
      val observer = mock[StreamObserver[RenewTokenResponse]]
      val errorCaptor = ArgumentCaptor.forClass(classOf[Throwable])

      service.renewToken(
        RenewTokenRequest.newBuilder().setToken("nonexistent-token").build(),
        observer)

      verify(observer).onError(errorCaptor.capture())
      val status = Status.fromThrowable(errorCaptor.getValue)
      assert(status.getCode === Status.Code.UNAUTHENTICATED)
      assert(status.getDescription.contains("Token not found or expired"))
    } finally {
      store.stop()
    }
  }

  // revokeToken

  test("revokeToken: valid token is revoked and response is empty") {
    val store = new InMemoryTokenStore(TTL_MS)
    try {
      val (token, _) = store.create("john")
      val service = new SparkConnectAuthServiceImpl(handlers = Seq.empty, store)
      val observer = mock[StreamObserver[RevokeTokenResponse]]

      service.revokeToken(RevokeTokenRequest.newBuilder().setToken(token).build(), observer)

      verify(observer).onNext(RevokeTokenResponse.getDefaultInstance)
      verify(observer).onCompleted()
      assert(store.getUser(token) === None)
    } finally {
      store.stop()
    }
  }

  test("revokeToken: unknown token succeeds (no-op)") {
    val store = new InMemoryTokenStore(TTL_MS)
    try {
      val service = new SparkConnectAuthServiceImpl(handlers = Seq.empty, store)
      val observer = mock[StreamObserver[RevokeTokenResponse]]

      service.revokeToken(
        RevokeTokenRequest.newBuilder().setToken("nonexistent-token").build(),
        observer)

      verify(observer).onNext(any())
      verify(observer).onCompleted()
    } finally {
      store.stop()
    }
  }
}
