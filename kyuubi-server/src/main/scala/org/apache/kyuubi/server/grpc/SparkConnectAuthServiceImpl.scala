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

import io.grpc.Status
import io.grpc.stub.StreamObserver

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.grpc.SparkConnectAuthServiceImpl.NegotiateHeader
import org.apache.kyuubi.server.grpc.proto._
import org.apache.kyuubi.server.grpc.proto.SparkConnectAuthServiceGrpc
import org.apache.kyuubi.server.http.util.HttpAuthUtils.NEGOTIATE

/**
 * gRPC service implementation for issuing, renewing and revoking Spark Connect session tokens.
 *
 * This service is NOT registered behind SparkConnectAuthInterceptor. it performs its own
 * SPNEGO validation via SparkConnectRawHeaderContext (Authorization: Negotiate <token>)
 * to get token (getToken). RenewToken and RevokeToken methods accept valid existing token.
 */
class SparkConnectAuthServiceImpl(
    validator: SparkConnectKerberosValidator,
    store: SparkConnectTokenStore)
  extends SparkConnectAuthServiceGrpc.SparkConnectAuthServiceImplBase
  with Logging {

  override def getToken(
      request: GetTokenRequest,
      observer: StreamObserver[GetTokenResponse]): Unit = {
    val spnegoToken = Option(SparkConnectRawHeaderContext.AUTH_HEADER_KEY.get()) match {
      case Some(NegotiateHeader(token)) => token
      case _ =>
        observer.onError(Status.UNAUTHENTICATED
          .withDescription(
            "GetToken requires Kerberos authentication: Authorization: Negotiate <spnego-token>")
          .asRuntimeException())
        return
    }
    try {
      val username = validator.validate(spnegoToken)
      val (token, expiresAtMs) = store.create(username)
      info(s"Issued Connect token for user $username")
      observer.onNext(GetTokenResponse.newBuilder()
        .setToken(token)
        .setExpiresAtMs(expiresAtMs)
        .build())
      observer.onCompleted()
    } catch {
      case e: Exception =>
        warn(s"GetToken SPNEGO validation failed: ${e.getMessage}")
        observer.onError(Status.UNAUTHENTICATED
          .withDescription("Kerberos authentication failed: " + e.getMessage)
          .asRuntimeException())
    }
  }

  override def renewToken(
      request: RenewTokenRequest,
      observer: StreamObserver[RenewTokenResponse]): Unit = {
    store.renew(request.getToken) match {
      case Some(newExpiry) =>
        observer.onNext(RenewTokenResponse.newBuilder().setExpiresAtMs(newExpiry).build())
        observer.onCompleted()
      case None =>
        observer.onError(Status.UNAUTHENTICATED
          .withDescription("Token not found or expired")
          .asRuntimeException())
    }
  }

  override def revokeToken(
      request: RevokeTokenRequest,
      observer: StreamObserver[RevokeTokenResponse]): Unit = {
    store.revoke(request.getToken)
    observer.onNext(RevokeTokenResponse.newBuilder().build())
    observer.onCompleted()
  }
}

object SparkConnectAuthServiceImpl {
  private object NegotiateHeader {
    def unapply(header: String): Option[String] =
      if (header.startsWith(s"$NEGOTIATE ")) Some(header.stripPrefix(s"$NEGOTIATE "))
      else None
  }
}
