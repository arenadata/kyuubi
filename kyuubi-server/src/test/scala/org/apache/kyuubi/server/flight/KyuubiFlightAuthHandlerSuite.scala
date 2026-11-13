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

package org.apache.kyuubi.server.flight

import java.util
import java.util.Base64

import org.apache.arrow.flight.{CallHeaders, FlightRuntimeException, FlightStatusCode}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{AUTHENTICATION_CUSTOM_CLASS, AUTHENTICATION_LDAP_BASE_DN, AUTHENTICATION_LDAP_URL, AUTHENTICATION_METHOD}
import org.apache.kyuubi.service.authentication.UserDefineAuthenticationProviderImpl

class KyuubiFlightAuthHandlerSuite extends KyuubiFunSuite {

  test("anonymous auth when authentication is disabled") {
    val conf = KyuubiConf().set(AUTHENTICATION_METHOD, Seq("NONE"))
    val auth = new KyuubiFlightAuthHandler(conf)
    val headers = new MapCallHeaders
    assert(auth.authenticate(headers).getPeerIdentity === "anonymous")

    headers.insert("x-user-name", "alice")
    assert(auth.authenticate(headers).getPeerIdentity === "alice")
  }

  test("missing credentials fail when auth is required") {
    val conf = KyuubiConf().set(AUTHENTICATION_METHOD, Seq("LDAP"))
    val auth = new KyuubiFlightAuthHandler(conf)
    intercept[RuntimeException] {
      auth.authenticate(new MapCallHeaders)
    }
  }

  test("malformed basic credentials are rejected") {
    val conf = KyuubiConf().set(AUTHENTICATION_METHOD, Seq("LDAP"))
    val auth = new KyuubiFlightAuthHandler(conf)
    val headers = new MapCallHeaders
    headers.insert(
      "authorization",
      "Basic " + Base64.getEncoder.encodeToString("nouser".getBytes("UTF-8")))
    intercept[RuntimeException] {
      auth.authenticate(headers)
    }
  }

  test("wrong basic credentials are UNAUTHENTICATED, valid ones authenticate") {
    val conf = KyuubiConf()
      .set(AUTHENTICATION_METHOD, Seq("CUSTOM"))
      .set(
        AUTHENTICATION_CUSTOM_CLASS,
        classOf[UserDefineAuthenticationProviderImpl].getCanonicalName)
    val auth = new KyuubiFlightAuthHandler(conf)

    def basic(user: String, password: String): MapCallHeaders = {
      val headers = new MapCallHeaders
      headers.insert(
        "authorization",
        "Basic " + Base64.getEncoder.encodeToString(s"$user:$password".getBytes("UTF-8")))
      headers
    }

    val e = intercept[FlightRuntimeException] {
      auth.authenticate(basic("user", "wrong"))
    }
    assert(e.status().code() === FlightStatusCode.UNAUTHENTICATED)
    assert(e.getMessage.contains("Invalid username or password"))
    assert(!e.getMessage.contains("wrong"), "the password must not leak into the error")

    assert(auth.authenticate(basic("user", "password")).getPeerIdentity === "user")
  }

  test("an unreachable LDAP server is UNAVAILABLE, not UNAUTHENTICATED") {
    val conf = KyuubiConf()
      .set(AUTHENTICATION_METHOD, Seq("LDAP"))
      // nothing listens on port 1: the connection is refused at once
      .set(AUTHENTICATION_LDAP_URL, "ldap://127.0.0.1:1")
      .set(AUTHENTICATION_LDAP_BASE_DN, "ou=people,dc=example,dc=com")
    val auth = new KyuubiFlightAuthHandler(conf)
    val headers = new MapCallHeaders
    headers.insert(
      "authorization",
      "Basic " + Base64.getEncoder.encodeToString("alice:secret".getBytes("UTF-8")))
    val e = intercept[FlightRuntimeException](auth.authenticate(headers))
    assert(e.status().code() === FlightStatusCode.UNAVAILABLE, e.getMessage)
    assert(!e.getMessage.contains("secret"), "the password must not leak into the error")
  }

  test("only connection failures in the cause chain count as an unavailable provider") {
    import KyuubiFlightAuthHandler.isServiceUnavailable
    val refused = new javax.naming.CommunicationException("127.0.0.1:1")
    refused.setRootCause(new java.net.ConnectException("Connection refused"))
    val wrapped = new IllegalStateException(
      new javax.security.sasl.AuthenticationException("Error validating LDAP user", refused))
    assert(isServiceUnavailable(wrapped))
    assert(isServiceUnavailable(new java.net.SocketTimeoutException("Read timed out")))

    val badPassword = new IllegalStateException(new javax.security.sasl.AuthenticationException(
      "Error validating LDAP user",
      new javax.naming.AuthenticationException("[LDAP: error code 49 - Invalid Credentials]")))
    assert(!isServiceUnavailable(badPassword))
    assert(!isServiceUnavailable(new javax.security.sasl.AuthenticationException("no such user")))
  }

  private class MapCallHeaders extends CallHeaders {
    private val values = new util.LinkedHashMap[String, String]()

    override def get(key: String): String = values.get(key.toLowerCase)

    override def getByte(key: String): Array[Byte] = null

    override def getAll(key: String): java.lang.Iterable[String] = {
      val value = values.get(key.toLowerCase)
      if (value == null) util.Collections.emptyList()
      else util.Collections.singletonList(value)
    }

    override def getAllByte(key: String): java.lang.Iterable[Array[Byte]] =
      util.Collections.emptyList()

    override def insert(key: String, value: String): Unit =
      values.put(key.toLowerCase, value)

    override def insert(key: String, value: Array[Byte]): Unit = ()

    override def keys(): util.Set[String] = values.keySet()

    override def containsKey(key: String): Boolean = values.containsKey(key.toLowerCase)
  }
}
