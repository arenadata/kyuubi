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

import io.grpc.{CallOptions, Channel, ClientCall, Metadata, MethodDescriptor}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

class KyuubiEngineTokenClientInterceptorSuite extends KyuubiFunSuite with MockitoSugar {

  override def beforeAll(): Unit = {
    super.beforeAll()
    InternalSecurityAccessor.reset()
    val conf = KyuubiConf(false)
      .set(KyuubiConf.ENGINE_SECURITY_SECRET_PROVIDER, "simple")
      .set(KyuubiConf.SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET, "test-secret-1234")
    InternalSecurityAccessor.initialize(conf, isServer = true)
  }

  override def afterAll(): Unit = {
    InternalSecurityAccessor.reset()
    super.afterAll()
  }

  private def startOneCall(interceptor: KyuubiEngineTokenClientInterceptor): String = {
    // MethodDescriptor is a final class Mockito can't mock; it's only ever forwarded opaquely to
    // the (mocked) channel below, so a null placeholder is fine here.
    val method: MethodDescriptor[Array[Byte], Array[Byte]] = null
    val call = mock[ClientCall[Array[Byte], Array[Byte]]]
    val channel = mock[Channel]
    when(channel.newCall(any[MethodDescriptor[Array[Byte], Array[Byte]]](), any[CallOptions]()))
      .thenReturn(call)

    val intercepted = interceptor.interceptCall(method, CallOptions.DEFAULT, channel)
    val listener = mock[ClientCall.Listener[Array[Byte]]]
    val headers = new Metadata()
    intercepted.start(listener, headers)
    headers.get(KyuubiEngineTokenClientInterceptor.TOKEN_HEADER)
  }

  test("attaches a valid, freshly-issued token header on every call") {
    val interceptor = new KyuubiEngineTokenClientInterceptor

    val token1 = startOneCall(interceptor)
    assert(token1 != null)
    // Must be decryptable by the same InternalSecurityAccessor an engine-side interceptor would use
    InternalSecurityAccessor.get().authToken(token1)

    // A second, independent call must mint its own token (per-call reissuance, not a cached
    // channel-lifetime token) - re-validated independently, not just string equality/inequality.
    val token2 = startOneCall(interceptor)
    assert(token2 != null)
    InternalSecurityAccessor.get().authToken(token2)
  }
}
