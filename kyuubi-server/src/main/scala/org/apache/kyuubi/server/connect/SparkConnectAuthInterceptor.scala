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

package org.apache.kyuubi.server.connect

import java.util.Base64

import io.grpc.{Context, Contexts, Metadata, ServerCall, ServerCallHandler, ServerInterceptor, Status}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.AUTHENTICATION_METHOD
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, AuthMethods, AuthTypes, AuthUtils}

class SparkConnectAuthInterceptor(conf: KyuubiConf) extends ServerInterceptor with Logging {

  val AUTH_HEADER: Metadata.Key[String] =
    Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)

  val USER_KEY: Context.Key[String] = SparkConnectAuthInterceptor.USER_KEY

  private val authTypes = conf.get(AUTHENTICATION_METHOD).map(AuthTypes.withName)
  private val saslDisabled = AuthUtils.saslDisabled(authTypes)
  private val effectivePlainAuthType = AuthUtils.effectivePlainAuthType(authTypes)

  private val authProvider = effectivePlainAuthType match {
    case Some(authType) =>
      val method = AuthMethods.withName(authType.toString)
      Some(AuthenticationProviderFactory.getAuthenticationProvider(method, conf, isServer = true))
    case None => None
  }

  override def interceptCall[Req, Resp](
      call: ServerCall[Req, Resp],
      headers: Metadata,
      next: ServerCallHandler[Req, Resp]): ServerCall.Listener[Req] = {
    Option(headers.get(AUTH_HEADER)) match {
      case Some(authHeader) =>
        val (user, pass) = decodeBasic(authHeader)
        try {
          authProvider.foreach(_.authenticate(user, pass))
          val ctx = Context.current().withValue(USER_KEY, user)
          Contexts.interceptCall(ctx, call, headers, next)
        } catch {
          case e: Exception =>
            warn(s"Authentication failed for user $user: ${e.getMessage}")
            call.close(
              Status.UNAUTHENTICATED.withDescription("Authentication failed: " + e.getMessage),
              new Metadata())
            new ServerCall.Listener[Req] {}
        }
      case None if saslDisabled =>
        // NOSASL mode: extract username from x-user-name header or fall back to OS user
        val xUserNameKey = Metadata.Key.of("x-user-name", Metadata.ASCII_STRING_MARSHALLER)
        val user = Option(headers.get(xUserNameKey))
          .getOrElse(System.getProperty("user.name", "anonymous"))
        val ctx = Context.current().withValue(USER_KEY, user)
        Contexts.interceptCall(ctx, call, headers, next)
      case None =>
        call.close(
          Status.UNAUTHENTICATED.withDescription("Missing Authorization header"),
          new Metadata())
        new ServerCall.Listener[Req] {}
    }
  }

  private def decodeBasic(header: String): (String, String) = {
    val decoded = new String(Base64.getDecoder.decode(header.stripPrefix("Basic ")))
    val idx = decoded.indexOf(':')
    if (idx < 0) (decoded, "")
    else (decoded.substring(0, idx), decoded.substring(idx + 1))
  }
}

object SparkConnectAuthInterceptor {
  val USER_KEY: Context.Key[String] = Context.key("kyuubi.connect.user")
}
