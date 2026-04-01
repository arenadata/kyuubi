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

import javax.security.sasl.AuthenticationException

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.KerberizedTestHelper
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{SERVER_SPNEGO_KEYTAB, SERVER_SPNEGO_PRINCIPAL}

class SparkConnectKerberosValidatorSuite extends KerberizedTestHelper {

  private def makeValidator(): SparkConnectKerberosValidator = {
    val conf = KyuubiConf()
      .set(SERVER_SPNEGO_KEYTAB, testKeytab)
      .set(SERVER_SPNEGO_PRINCIPAL, testSpnegoPrincipal)
    new SparkConnectKerberosValidator(conf)
  }

  test("validate: real SPNEGO token returns short username") {
    tryWithSecurityEnabled {
      UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)
      val validator = makeValidator()
      val token = generateToken(hostName)
      val username = validator.validate(token)
      assert(username === clientPrincipalUser)
    }
  }

  test("validate: not valid token throws AuthenticationException") {
    tryWithSecurityEnabled {
      val validator = makeValidator()
      intercept[AuthenticationException] {
        validator.validate("bm90YXZhbGlkdG9rZW4=") // base64("notavalidtoken")
      }
    }
  }
}
