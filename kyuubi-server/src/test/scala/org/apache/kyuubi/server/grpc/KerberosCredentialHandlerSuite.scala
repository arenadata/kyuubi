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

import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{verify, when}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.kyuubi.KyuubiFunSuite

class KerberosCredentialHandlerSuite extends KyuubiFunSuite with MockitoSugar {

  test("Negotiate header delegates to validator and returns Some(username)") {
    val validator = mock[SparkConnectKerberosValidator]
    when(validator.validate("dG9rZW4=")).thenReturn("john")
    val handler = new KerberosCredentialHandler(validator)

    val result = handler.authenticate("Negotiate dG9rZW4=")

    assert(result === Some("john"))
    verify(validator).validate("dG9rZW4=")
  }

  test("Negotiate header with validator throwing propagates exception") {
    val validator = mock[SparkConnectKerberosValidator]
    when(validator.validate(anyString())).thenThrow(new RuntimeException("bad token"))
    val handler = new KerberosCredentialHandler(validator)

    val ex = intercept[RuntimeException] {
      handler.authenticate("Negotiate dG9rZW4=")
    }
    assert(ex.getMessage === "bad token")
  }

  test("non-Negotiate scheme returns None without calling validator") {
    val validator = mock[SparkConnectKerberosValidator]
    val handler = new KerberosCredentialHandler(validator)

    val result = handler.authenticate("Basic dXNlcjpwYXNz")

    assert(result === None)
    org.mockito.Mockito.verifyNoInteractions(validator)
  }

  test("Bearer scheme returns None without calling validator") {
    val validator = mock[SparkConnectKerberosValidator]
    val handler = new KerberosCredentialHandler(validator)

    val result = handler.authenticate("Bearer some-uuid")

    assert(result === None)
    org.mockito.Mockito.verifyNoInteractions(validator)
  }
}
