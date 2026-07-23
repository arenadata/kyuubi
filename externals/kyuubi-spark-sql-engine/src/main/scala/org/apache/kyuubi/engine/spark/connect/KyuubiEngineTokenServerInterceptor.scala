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

import org.sparkproject.connect.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor, Status}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

// Keep a public no-arg constructor: Spark loads this class by name (from
// spark.connect.grpc.interceptor.classes) and instantiates it in
// SparkConnectInterceptorRegistry.createInstance
//
// Must implement org.sparkproject.connect.grpc.ServerInterceptor (Spark Connect's shaded/
// relocated grpc, baked into the spark-connect jar), not plain io.grpc.ServerInterceptor -
// SparkConnectInterceptorRegistry.createInstance casts to the shaded type, and since shading
// makes them genuinely distinct classes (not just different classloaders of the same class),
// implementing the unshaded interface fails with a ClassCastException at engine startup.
class KyuubiEngineTokenServerInterceptor extends ServerInterceptor with Logging {

  override def interceptCall[Req, Resp](
      call: ServerCall[Req, Resp],
      headers: Metadata,
      next: ServerCallHandler[Req, Resp]): ServerCall.Listener[Req] = {
    Option(headers.get(KyuubiEngineTokenServerInterceptor.TOKEN_HEADER)) match {
      case Some(token) =>
        try {
          InternalSecurityAccessor.get().authToken(token)
          next.startCall(call, headers)
        } catch {
          case e: Exception =>
            call.close(Status.UNAUTHENTICATED.withDescription(e.getMessage), new Metadata())
            new ServerCall.Listener[Req] {}
        }
      case None =>
        call.close(
          Status.UNAUTHENTICATED.withDescription("Missing Kyuubi engine token"),
          new Metadata())
        new ServerCall.Listener[Req] {}
    }
  }
}

object KyuubiEngineTokenServerInterceptor {
  val TOKEN_HEADER: Metadata.Key[String] =
    Metadata.Key.of("x-kyuubi-engine-token", Metadata.ASCII_STRING_MARSHALLER)
}
