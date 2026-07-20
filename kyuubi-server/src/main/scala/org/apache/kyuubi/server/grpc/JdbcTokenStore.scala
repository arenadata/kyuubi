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

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.metadata.jdbc.DatabaseType
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.util.JdbcUtils
import org.apache.kyuubi.util.reflect.ReflectUtils

/**
 * JDBC-backed implementation of [[SparkConnectTokenStore]].
 *
 * Tokens are persisted in the `spark_connect_tokens` table in the same database configured for
 * the Kyuubi metadata store (`kyuubi.metadata.store.jdbc.*`). This makes tokens visible to all
 * nodes in an HA cluster - a token issued by server A is accepted by server B after failover
 * without requiring re-authentication.
 *
 * A local ConcurrentHashMap cache avoids a DB round-trip on every gRPC interceptor call.
 * The cache is populated on first access and invalidated on revoke/expiry.
 * Background thread deletes expired rows from DB every 10 minutes.
 *
 * The `spark_connect_tokens` table must already exist (created by the schema init or migration).
 */
class JdbcTokenStore(
    conf: KyuubiConf,
    val ttlMs: Long,
    cacheFreshnessMs: Long = JdbcTokenStore.CacheFreshnessMs)
  extends SparkConnectTokenStore with Logging {

  // `cachedAt` records when this entry was last loaded/refreshed from the DB. It is used to
  // decide whether a local cache hit is fresh enough to be trusted without a DB round-trip.
  private case class Entry(username: String, expiresAt: Long, cachedAt: Long)

  private val cache = new ConcurrentHashMap[String, Entry]()

  implicit private val ds: HikariDataSource = {
    val props = JDBCMetadataStoreConf.getMetadataStoreJDBCDataSourceProperties(conf)
    val hc = new HikariConfig(props)
    hc.setDriverClassName(resolveDriver())
    hc.setJdbcUrl(JDBCMetadataStoreConf.getMetadataStoreJdbcUrl(conf))
    hc.setUsername(conf.get(METADATA_STORE_JDBC_USER))
    hc.setPassword(conf.get(METADATA_STORE_JDBC_PASSWORD))
    hc.setMaximumPoolSize(3)
    hc.setPoolName("connect-token-store-pool")
    new HikariDataSource(hc)
  }

  private val scheduler = Executors.newSingleThreadScheduledExecutor(r => {
    val t = new Thread(r, "connect-token-cleaner")
    t.setDaemon(true)
    t
  })
  scheduler.scheduleAtFixedRate(() => deleteExpired(), 10, 10, TimeUnit.MINUTES)

  override def create(username: String): (String, Long) = {
    val token = UUID.randomUUID().toString
    val now = System.currentTimeMillis()
    val expiresAt = now + ttlMs
    JdbcUtils.executeUpdate(
      "INSERT INTO spark_connect_tokens(token_id, username, created_at, expires_at)" +
        " VALUES(?,?,?,?)"
    ) { stmt =>
      stmt.setString(1, token)
      stmt.setString(2, username)
      stmt.setLong(3, now)
      stmt.setLong(4, expiresAt)
    }
    cache.put(token, Entry(username, expiresAt, now))
    debug(s"Created Connect token for user $username")
    (token, expiresAt)
  }

  override def getUser(token: String): Option[String] = {
    val now = System.currentTimeMillis()
    Option(cache.get(token)) match {
      // Trust a cache hit only if it is both unexpired and fresh (loaded from the DB within the
      // freshness window). A read must never have a destructive side effect based on possibly
      // stale local state - so on a miss or a stale/expired entry we treat the DB as truth and
      // never delete here (expired rows are swept by deleteExpired() / removed by revoke()).
      case Some(entry) if entry.expiresAt > now && now - entry.cachedAt <= cacheFreshnessMs =>
        Some(entry.username)
      case _ =>
        loadFromDb(token, now) match {
          case Some(entry) =>
            cache.put(token, entry)
            Some(entry.username)
          case None =>
            cache.remove(token)
            None
        }
    }
  }

  override def renew(token: String): Option[Long] = {
    val now = System.currentTimeMillis()
    // Use a fresh, unexpired cache entry directly; otherwise reload from the DB and use that as
    // the source of truth. Expiry is never confirmed via a stale cache alone, so a locally-stale
    // entry can never cause a token that another node renewed to be dropped/deleted.
    val current = Option(cache.get(token))
      .filter(e => e.expiresAt > now && now - e.cachedAt <= cacheFreshnessMs)
      .orElse(loadFromDb(token, now))
    current match {
      case Some(entry) if entry.expiresAt > now =>
        val newExpiry = now + ttlMs
        JdbcUtils.executeUpdate(
          "UPDATE spark_connect_tokens SET expires_at=? WHERE token_id=?"
        ) { stmt =>
          stmt.setLong(1, newExpiry)
          stmt.setString(2, token)
        }
        cache.put(token, entry.copy(expiresAt = newExpiry, cachedAt = now))
        debug(s"Renewed Connect token for user ${entry.username}")
        Some(newExpiry)
      case _ =>
        cache.remove(token)
        None
    }
  }

  override def revoke(token: String): Unit = {
    cache.remove(token)
    deleteToken(token)
    debug(s"Revoked Connect token $token")
  }

  override def stop(): Unit = {
    scheduler.shutdownNow()
    cache.clear()
    ds.close()
  }

  private def loadFromDb(token: String, now: Long): Option[Entry] = {
    JdbcUtils.executeQuery(
      "SELECT username, expires_at FROM spark_connect_tokens WHERE token_id=? AND expires_at>?"
    ) { stmt =>
      stmt.setString(1, token)
      stmt.setLong(2, now)
    } { rs =>
      if (rs.next()) Some(Entry(rs.getString("username"), rs.getLong("expires_at"), now))
      else None
    }
  }

  private def deleteToken(token: String): Unit = {
    JdbcUtils.executeUpdate("DELETE FROM spark_connect_tokens WHERE token_id=?") { stmt =>
      stmt.setString(1, token)
    }
  }

  private def deleteExpired(): Unit = {
    val now = System.currentTimeMillis()
    cache.entrySet().removeIf(_.getValue.expiresAt <= now)
    val deleted = JdbcUtils.executeUpdate(
      "DELETE FROM spark_connect_tokens WHERE expires_at<=?"
    ) { stmt =>
      stmt.setLong(1, now)
    }
    if (deleted > 0) info(s"Removed $deleted expired Connect tokens from DB")
  }

  private def resolveDriver(): String = {
    import DatabaseType._
    val dbType = DatabaseType.withName(conf.get(METADATA_STORE_JDBC_DATABASE_TYPE))
    conf.get(METADATA_STORE_JDBC_DRIVER).getOrElse(dbType match {
      case SQLITE => "org.sqlite.JDBC"
      case MYSQL =>
        if (ReflectUtils.isClassLoadable("com.mysql.cj.jdbc.Driver")) "com.mysql.cj.jdbc.Driver"
        else "com.mysql.jdbc.Driver"
      case POSTGRESQL => "org.postgresql.Driver"
      case CUSTOM => throw new IllegalArgumentException(
        s"${METADATA_STORE_JDBC_DRIVER.key} must be set for CUSTOM database type")
    })
  }
}

object JdbcTokenStore {

  // How long a locally cached entry is trusted before it must be re-validated against the shared
  // DB. Kept short so that renewals/revocations performed on other HA nodes become visible here
  // within this window. Not a KyuubiConf entry on purpose - a private constant is sufficient.
  private[grpc] val CacheFreshnessMs = 30000L
}
