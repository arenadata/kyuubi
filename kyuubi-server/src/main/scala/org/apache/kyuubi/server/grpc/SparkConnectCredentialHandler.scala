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

import SparkConnectCredentialHandler.BASIC_PREFIX
import SparkConnectCredentialHandler.NEGOTIATE_PREFIX

import org.apache.kyuubi.server.http.util.HttpAuthUtils.BASIC
import org.apache.kyuubi.server.http.util.HttpAuthUtils.NEGOTIATE
import org.apache.kyuubi.service.authentication.PasswdAuthenticationProvider

/**
 * Authenticates raw Authorization header value and returns the username.
 *
 * Returns Some(username) if the handler recognizes the scheme and authentication succeeds.
 * Returns None if the scheme is not recognized by this handler.
 */
trait SparkConnectCredentialHandler {
  def authenticate(authHeader: String): Option[String]
}

object SparkConnectCredentialHandler {
  val NEGOTIATE_PREFIX: String = s"$NEGOTIATE "
  val BASIC_PREFIX: String = s"$BASIC "
  val BEARER_PREFIX: String = "Bearer "
}

class KerberosCredentialHandler(validator: SparkConnectKerberosValidator)
  extends SparkConnectCredentialHandler {

  override def authenticate(authHeader: String): Option[String] = {
    if (authHeader.startsWith(NEGOTIATE_PREFIX)) {
      Some(validator.validate(authHeader.stripPrefix(NEGOTIATE_PREFIX)))
    } else {
      None
    }
  }
}

class BasicCredentialHandler(provider: PasswdAuthenticationProvider)
  extends SparkConnectCredentialHandler {

  override def authenticate(authHeader: String): Option[String] =
    if (authHeader.startsWith(BASIC_PREFIX)) {
      val (user, password) = decodeCredentials(authHeader.stripPrefix(BASIC_PREFIX))
      provider.authenticate(user, password)
      Some(user)
    } else {
      None
    }

  private def decodeCredentials(base64token: String): (String, String) = {
    val decoded = new String(Base64.getDecoder.decode(base64token))
    val idx = decoded.indexOf(':')
    if (idx < 0) (decoded, "") else (decoded.substring(0, idx), decoded.substring(idx + 1))
  }
}
