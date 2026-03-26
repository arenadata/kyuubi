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

import io.grpc.{Context, Contexts, Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

/**
 * Captures raw Authorization header from gRPC metadata and stores it in the gRPC Context.
 * Used by SparkConnectAuthServiceImpl.
 */
object SparkConnectRawHeaderContext {

  val AUTH_HEADER_KEY: Context.Key[String] = Context.key("raw-authorization")

  private val METADATA_KEY =
    Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

  val interceptor: ServerInterceptor = new ServerInterceptor {
    override def interceptCall[Req, Resp](
        call: ServerCall[Req, Resp],
        headers: Metadata,
        next: ServerCallHandler[Req, Resp]): ServerCall.Listener[Req] = {
      val authValue = headers.get(METADATA_KEY)
      val ctx = Context.current().withValue(AUTH_HEADER_KEY, authValue)
      Contexts.interceptCall(ctx, call, headers, next)
    }
  }
}
