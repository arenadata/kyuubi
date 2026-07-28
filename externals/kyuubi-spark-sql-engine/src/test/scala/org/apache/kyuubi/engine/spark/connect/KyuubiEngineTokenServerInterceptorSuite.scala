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

package org.apache.kyuubi.engine.spark.connect

import org.sparkproject.connect.grpc.{Metadata, MethodDescriptor, ServerCall, ServerCallHandler, Status}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

class KyuubiEngineTokenServerInterceptorSuite extends KyuubiFunSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    InternalSecurityAccessor.reset()
    val conf = KyuubiConf(false)
      .set(KyuubiConf.ENGINE_SECURITY_SECRET_PROVIDER, "simple")
      .set(KyuubiConf.SIMPLE_SECURITY_SECRET_PROVIDER_PROVIDER_SECRET, "test-secret-1234")
    InternalSecurityAccessor.initialize(conf, isServer = false)
  }

  override def afterAll(): Unit = {
    InternalSecurityAccessor.reset()
    super.afterAll()
  }

  private class FakeServerCall extends ServerCall[Array[Byte], Array[Byte]] {
    var closedStatus: Status = _
    override def request(numMessages: Int): Unit = ()
    override def sendHeaders(headers: Metadata): Unit = ()
    override def sendMessage(message: Array[Byte]): Unit = ()
    override def close(status: Status, trailers: Metadata): Unit = { closedStatus = status }
    override def isReady: Boolean = true
    override def getAuthority: String = null
    override def isCancelled: Boolean = false
    override def getMethodDescriptor: MethodDescriptor[Array[Byte], Array[Byte]] = null
  }

  private class FakeHandler extends ServerCallHandler[Array[Byte], Array[Byte]] {
    var called = false
    override def startCall(
        call: ServerCall[Array[Byte], Array[Byte]],
        headers: Metadata): ServerCall.Listener[Array[Byte]] = {
      called = true
      new ServerCall.Listener[Array[Byte]] {}
    }
  }

  private def makeHeaders(token: Option[String]): Metadata = {
    val headers = new Metadata()
    token.foreach(headers.put(KyuubiEngineTokenServerInterceptor.TOKEN_HEADER, _))
    headers
  }

  test("missing token returns UNAUTHENTICATED") {
    val interceptor = new KyuubiEngineTokenServerInterceptor
    val call = new FakeServerCall
    val handler = new FakeHandler

    interceptor.interceptCall(call, makeHeaders(None), handler)

    assert(!handler.called)
    assert(call.closedStatus.getCode === Status.Code.UNAUTHENTICATED)
    assert(call.closedStatus.getDescription.contains("Missing Kyuubi engine token"))
  }

  test("valid token lets the call proceed") {
    val interceptor = new KyuubiEngineTokenServerInterceptor
    val call = new FakeServerCall
    val handler = new FakeHandler

    val token = InternalSecurityAccessor.get().issueToken()
    interceptor.interceptCall(call, makeHeaders(Some(token)), handler)

    assert(handler.called)
    assert(call.closedStatus == null)
  }

  test("garbage token returns UNAUTHENTICATED") {
    val interceptor = new KyuubiEngineTokenServerInterceptor
    val call = new FakeServerCall
    val handler = new FakeHandler

    interceptor.interceptCall(call, makeHeaders(Some("not-a-real-token")), handler)

    assert(!handler.called)
    assert(call.closedStatus.getCode === Status.Code.UNAUTHENTICATED)
  }
}
