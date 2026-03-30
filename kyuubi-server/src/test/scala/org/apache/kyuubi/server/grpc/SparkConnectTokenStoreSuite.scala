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

import org.apache.kyuubi.KyuubiFunSuite

class SparkConnectTokenStoreSuite extends KyuubiFunSuite {

  private val TTL_MS = 60000L // 1 minute

  test("create returns non-empty token and expiry in the future") {
    val store = new SparkConnectTokenStore(TTL_MS)
    try {
      val before = System.currentTimeMillis()
      val (token, expiresAt) = store.create("john")
      val after = System.currentTimeMillis()

      assert(token.nonEmpty)
      assert(expiresAt >= before + TTL_MS)
      assert(expiresAt <= after + TTL_MS)
    } finally {
      store.stop()
    }
  }

  test("getUser returns username for live token") {
    val store = new SparkConnectTokenStore(TTL_MS)
    try {
      val (token, _) = store.create("john")
      assert(store.getUser(token) === Some("john"))
    } finally {
      store.stop()
    }
  }

  test("getUser returns None for unknown token") {
    val store = new SparkConnectTokenStore(TTL_MS)
    try {
      assert(store.getUser("nonexistent-token") === None)
    } finally {
      store.stop()
    }
  }

  test("getUser returns None and removes expired token") {
    val store = new SparkConnectTokenStore(ttlMs = -1L) // already expired on creation
    try {
      val (token, _) = store.create("john")
      assert(store.getUser(token) === None)
      // second call should still return None (token was removed)
      assert(store.getUser(token) === None)
    } finally {
      store.stop()
    }
  }

  test("renew extends expiry of live token") {
    val store = new SparkConnectTokenStore(TTL_MS)
    try {
      val (token, originalExpiry) = store.create("john")
      Thread.sleep(5)
      val renewedExpiry = store.renew(token)
      assert(renewedExpiry.isDefined)
      assert(renewedExpiry.get > originalExpiry)
    } finally {
      store.stop()
    }
  }

  test("renew returns None for expired token") {
    val store = new SparkConnectTokenStore(ttlMs = -1L)
    try {
      val (token, _) = store.create("john")
      assert(store.renew(token) === None)
    } finally {
      store.stop()
    }
  }

  test("renew returns None for unknown token") {
    val store = new SparkConnectTokenStore(TTL_MS)
    try {
      assert(store.renew("nonexistent-token") === None)
    } finally {
      store.stop()
    }
  }

  test("revoke makes token invalid") {
    val store = new SparkConnectTokenStore(TTL_MS)
    try {
      val (token, _) = store.create("john")
      store.revoke(token)
      assert(store.getUser(token) === None)
    } finally {
      store.stop()
    }
  }

  test("revoke unknown token is a no-op") {
    val store = new SparkConnectTokenStore(TTL_MS)
    try {
      store.revoke("nonexistent-token") // must not throw
    } finally {
      store.stop()
    }
  }

  test("multiple tokens for different users are independent") {
    val store = new SparkConnectTokenStore(TTL_MS)
    try {
      val (tokenJohn, _) = store.create("john")
      val (tokenMark, _) = store.create("mark")

      assert(store.getUser(tokenJohn) === Some("john"))
      assert(store.getUser(tokenMark) === Some("mark"))

      store.revoke(tokenJohn)
      assert(store.getUser(tokenJohn) === None)
      assert(store.getUser(tokenMark) === Some("mark"))
    } finally {
      store.stop()
    }
  }

  test("stop clears all tokens") {
    val store = new SparkConnectTokenStore(TTL_MS)
    val (token, _) = store.create("john")
    store.stop()
    assert(store.getUser(token) === None)
  }
}
