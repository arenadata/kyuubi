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
import javax.security.sasl.AuthenticationException

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{AUTHENTICATION_LDAP_BASE_DN, AUTHENTICATION_LDAP_URL}
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, AuthMethods, WithLdapServer}

class BasicCredentialHandlerLdapSuite extends WithLdapServer {

  private def basic(userAndPassword: String): String =
    s"Basic ${Base64.getEncoder.encodeToString(userAndPassword.getBytes)}"

  private def makeHandler(): BasicCredentialHandler = {
    val conf = KyuubiConf()
      .set(AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(AUTHENTICATION_LDAP_BASE_DN, "ou=users")
    new BasicCredentialHandler(
      AuthenticationProviderFactory.getAuthenticationProvider(AuthMethods.LDAP, conf))
  }

  test("valid LDAP credentials return Some(user)") {
    val handler = makeHandler()
    val result = handler.authenticate(basic(s"$ldapUser:$ldapUserPasswd"))
    assert(result === Some(ldapUser))
  }

  test("wrong password throws exception") {
    val handler = makeHandler()
    intercept[AuthenticationException] {
      handler.authenticate(basic(s"$ldapUser:wrongpassword"))
    }
  }

  test("non-Basic scheme returns None without contacting LDAP") {
    val handler = makeHandler()
    assert(handler.authenticate("Bearer sometoken") === None)
  }
}
