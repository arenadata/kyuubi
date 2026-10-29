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

package org.apache.kyuubi.gateway.session

import org.apache.kyuubi.KyuubiFunSuite

class JdbcRoutingSessionManagerSuite extends KyuubiFunSuite {

  import JdbcRoutingSessionManager._

  private val url = "jdbc:hive2://impala-a:21050/default"

  test("the caller's identity is appended to the connection url") {
    assert(
      impersonate(url, "alice", Some(DEFAULT_IMPERSONATION_TEMPLATE)) ===
        s"$url;hive.server2.proxy.user=alice")
  }

  test("a driver with its own spelling is configured, not coded for") {
    assert(
      impersonate(url, "alice", Some(";DelegationUID={user}")) === s"$url;DelegationUID=alice")
  }

  test("an identity already in the url is left alone") {
    val explicit = s"$url;hive.server2.proxy.user=service"
    assert(
      impersonate(explicit, "alice", Some(DEFAULT_IMPERSONATION_TEMPLATE)) === explicit,
      "two proxy users in one url mean whatever the driver reads first")
  }

  test("impersonation can be switched off, and then the url is untouched") {
    // An empty template is read as "off" before it gets here, so None is the
    // only way this is called when impersonation is disabled.
    assert(impersonate(url, "alice", None) === url)
  }
}
