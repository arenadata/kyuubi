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

import io.grpc._

import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

class KyuubiEngineTokenClientInterceptor extends ClientInterceptor {

  override def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel): ClientCall[ReqT, RespT] = {
    new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](
      next.newCall(method, callOptions)) {
      override def start(responseListener: ClientCall.Listener[RespT], headers: Metadata): Unit = {
        // Issue a fresh token per call, not once per channel: gRPC checks headers once at call
        // setup (not per message), but a long-lived channel can outlive the token's TTL between
        // calls, so each new call needs its own fresh token.
        headers.put(
          KyuubiEngineTokenClientInterceptor.TOKEN_HEADER,
          InternalSecurityAccessor.get().issueToken())
        super.start(responseListener, headers)
      }
    }
  }
}

object KyuubiEngineTokenClientInterceptor {
  val TOKEN_HEADER: Metadata.Key[String] =
    Metadata.Key.of("x-kyuubi-engine-token", Metadata.ASCII_STRING_MARSHALLER)
}
