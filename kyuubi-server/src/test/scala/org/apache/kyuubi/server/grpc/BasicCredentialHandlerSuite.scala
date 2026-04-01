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

import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{doThrow, verify}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.service.authentication.PasswdAuthenticationProvider

class BasicCredentialHandlerSuite extends KyuubiFunSuite with MockitoSugar {

  private def basic(userAndPassword: String): String =
    s"Basic ${Base64.getEncoder.encodeToString(userAndPassword.getBytes)}"

  test("valid Basic header calls provider with decoded user and password, returns Some(user)") {
    val provider = mock[PasswdAuthenticationProvider]
    val handler = new BasicCredentialHandler(provider)
    val userCaptor = ArgumentCaptor.forClass(classOf[String])
    val passCaptor = ArgumentCaptor.forClass(classOf[String])

    val result = handler.authenticate(basic("john:secret"))

    verify(provider).authenticate(userCaptor.capture(), passCaptor.capture())
    assert(result === Some("john"))
    assert(userCaptor.getValue === "john")
    assert(passCaptor.getValue === "secret")
  }

  test("token with no colon passes empty password") {
    val provider = mock[PasswdAuthenticationProvider]
    val handler = new BasicCredentialHandler(provider)
    val passCaptor = ArgumentCaptor.forClass(classOf[String])

    val result = handler.authenticate(basic("john"))

    verify(provider).authenticate(org.mockito.ArgumentMatchers.eq("john"), passCaptor.capture())
    assert(result === Some("john"))
    assert(passCaptor.getValue === "")
  }

  test("non-Basic scheme returns None without calling provider") {
    val provider = mock[PasswdAuthenticationProvider]
    val handler = new BasicCredentialHandler(provider)

    val result = handler.authenticate("Bearer sometoken")

    assert(result === None)
    org.mockito.Mockito.verifyNoInteractions(provider)
  }

  test("provider throwing propagates the exception") {
    val provider = mock[PasswdAuthenticationProvider]
    doThrow(new RuntimeException("bad credentials")).when(provider).authenticate("john", "wrong")
    val handler = new BasicCredentialHandler(provider)

    val ex = intercept[RuntimeException] {
      handler.authenticate(basic("john:wrong"))
    }
    assert(ex.getMessage === "bad credentials")
  }
}
