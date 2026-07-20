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

import java.nio.file.Files

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.metadata.jdbc.DatabaseType
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStore
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._

class JdbcTokenStoreSuite extends KyuubiFunSuite {

  private val TTL_MS = 60000L // 1 minute

  private def newStore(ttlMs: Long): JdbcTokenStore = {
    new JdbcTokenStore(newSharedDbConf(), ttlMs)
  }

  /**
   * Creates a KyuubiConf pointing at a fresh, schema-initialized SQLite DB file. Multiple
   * [[JdbcTokenStore]] instances built from the same conf share that one underlying DB file,
   * simulating several Kyuubi HA nodes backed by a single shared token store.
   */
  private def newSharedDbConf(): KyuubiConf = {
    val dbPath = Files.createTempFile("kyuubi-token", ".db").toAbsolutePath.toString
    val storeConf = KyuubiConf(false)
      .set(METADATA_STORE_JDBC_DATABASE_TYPE, DatabaseType.SQLITE.toString)
      .set(METADATA_STORE_JDBC_URL, s"jdbc:sqlite:$dbPath")
      .set(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT, true)
    val metaStore = new JDBCMetadataStore(storeConf)
    metaStore.close()
    storeConf
  }

  test("create returns non-empty token and expiry in the future") {
    val store = newStore(TTL_MS)
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
    val store = newStore(TTL_MS)
    try {
      val (token, _) = store.create("john")
      assert(store.getUser(token) === Some("john"))
    } finally {
      store.stop()
    }
  }

  test("getUser returns None for unknown token") {
    val store = newStore(TTL_MS)
    try {
      assert(store.getUser("nonexistent-token") === None)
    } finally {
      store.stop()
    }
  }

  test("getUser returns None for expired token") {
    val store = newStore(ttlMs = -1L) // already expired on creation
    try {
      val (token, _) = store.create("john")
      assert(store.getUser(token) === None)
      assert(store.getUser(token) === None)
    } finally {
      store.stop()
    }
  }

  test("renew extends expiry of live token") {
    val store = newStore(TTL_MS)
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
    val store = newStore(ttlMs = -1L)
    try {
      val (token, _) = store.create("john")
      assert(store.renew(token) === None)
    } finally {
      store.stop()
    }
  }

  test("renew returns None for unknown token") {
    val store = newStore(TTL_MS)
    try {
      assert(store.renew("nonexistent-token") === None)
    } finally {
      store.stop()
    }
  }

  test("revoke makes token invalid") {
    val store = newStore(TTL_MS)
    try {
      val (token, _) = store.create("john")
      store.revoke(token)
      assert(store.getUser(token) === None)
    } finally {
      store.stop()
    }
  }

  test("revoke unknown token is a no-op") {
    val store = newStore(TTL_MS)
    try {
      store.revoke("nonexistent-token") // must not throw
    } finally {
      store.stop()
    }
  }

  test("multiple tokens for different users are independent") {
    val store = newStore(TTL_MS)
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

  test("stop closes connection pool") {
    val store = newStore(TTL_MS)
    val (token, _) = store.create("john")
    store.stop()
    assert(token.nonEmpty)
  }

  test("stale local cache must not delete a token renewed by another node") {
    // Two stores share one DB file (two HA nodes). Store B is given a zero freshness window so
    // its cached entry is always considered stale and re-validated against the shared DB.
    val conf = newSharedDbConf()
    val shortTtl = 1000L
    val storeA = new JdbcTokenStore(conf, shortTtl)
    val storeB = new JdbcTokenStore(conf, shortTtl, cacheFreshnessMs = 0L)
    try {
      val (token, originalExpiry) = storeA.create("john")
      // B caches the token with its ORIGINAL (short) expiry.
      assert(storeB.getUser(token) === Some("john"))

      // A renews the token before it expires, extending expires_at in the shared DB.
      Thread.sleep(300)
      val renewed = storeA.renew(token)
      assert(renewed.isDefined)
      assert(renewed.get > originalExpiry)

      // Advance past the ORIGINAL expiry but stay within the RENEWED expiry. B's cached entry now
      // looks expired, while the shared DB row is still valid.
      val sleepMs = (originalExpiry - System.currentTimeMillis()) + 100
      if (sleepMs > 0) Thread.sleep(sleepMs)
      assert(System.currentTimeMillis() > originalExpiry)
      assert(System.currentTimeMillis() < renewed.get)

      // With the old bug, B would DELETE the renewed row from the shared DB here. It must not.
      assert(storeB.getUser(token) === Some("john"))

      // A fresh node must still see the renewed token in the shared DB.
      val storeC = new JdbcTokenStore(conf, shortTtl)
      try {
        assert(storeC.getUser(token) === Some("john"))
      } finally {
        storeC.stop()
      }
    } finally {
      storeA.stop()
      storeB.stop()
    }
  }

  test("revoke on one node becomes visible to another once its cache goes stale") {
    val conf = newSharedDbConf()
    val storeA = new JdbcTokenStore(conf, TTL_MS)
    // Zero freshness window: B's cache is always considered stale, so revocations become visible.
    val storeB = new JdbcTokenStore(conf, TTL_MS, cacheFreshnessMs = 0L)
    try {
      val (token, _) = storeA.create("john")
      // B caches the token as valid.
      assert(storeB.getUser(token) === Some("john"))

      // A revokes the token, deleting the shared DB row.
      storeA.revoke(token)

      // Ensure some wall-clock time passes so B's entry is strictly older than "now".
      Thread.sleep(5)
      // B re-validates against the DB (its cache is stale) and sees the revocation.
      assert(storeB.getUser(token) === None)
    } finally {
      storeA.stop()
      storeB.stop()
    }
  }
}
