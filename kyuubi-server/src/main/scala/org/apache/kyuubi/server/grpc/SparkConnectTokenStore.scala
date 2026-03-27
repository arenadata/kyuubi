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

import org.apache.kyuubi.Logging

/**
 * In-memory store for Spark Connect session tokens.
 * Each token is random UUID mapped to authenticated username and an expiry timestamp.
 * Background thread removes expired entries every 10 minutes.
 */
class SparkConnectTokenStore(val ttlMs: Long) extends Logging {

  private case class Entry(username: String, expiresAt: Long)

  private val tokens = new ConcurrentHashMap[String, Entry]()

  private val scheduler = Executors.newSingleThreadScheduledExecutor(r => {
    val thread = new Thread(r, "connect-token-cleaner")
    thread.setDaemon(true)
    thread
  })

  scheduler.scheduleAtFixedRate(
    () => removeExpired(),
    10,
    10,
    TimeUnit.MINUTES)

  /**
   * Creates new token for the given username and stores it with a TTL.
   * @return (token, expiresAtMs)
   */
  def create(username: String): (String, Long) = {
    val token = UUID.randomUUID().toString
    val expiresAt = System.currentTimeMillis() + ttlMs
    tokens.put(token, Entry(username, expiresAt))
    debug(s"Created Connect token for user $username")
    (token, expiresAt)
  }

  /**
   * Returns username for valid, non-expired token.
   * Removes token from the store if it has expired.
   */
  def getUser(token: String): Option[String] = {
    Option(tokens.get(token)).flatMap { entry =>
      if (entry.expiresAt > System.currentTimeMillis()) {
        Some(entry.username)
      } else {
        tokens.remove(token)
        None
      }
    }
  }

  /**
   * Extends TTL of existing token.
   * @return new expiry time in milliseconds, or None if token was not found or expired
   */
  def renew(token: String): Option[Long] = {
    Option(tokens.get(token)).flatMap { entry =>
      if (entry.expiresAt > System.currentTimeMillis()) {
        val newExpiry = System.currentTimeMillis() + ttlMs
        tokens.put(token, entry.copy(expiresAt = newExpiry))
        debug(s"Renewed Connect token for user ${entry.username}")
        Some(newExpiry)
      } else {
        tokens.remove(token)
        None
      }
    }
  }

  /**
   * Invalidates token.
   */
  def revoke(token: String): Unit = {
    Option(tokens.remove(token)).foreach { entry =>
      debug(s"Revoked Connect token for user ${entry.username}")
    }
  }

  /**
   * Shuts down the background cleaner and clears tokens.
   */
  def stop(): Unit = {
    scheduler.shutdownNow()
    tokens.clear()
  }

  private def removeExpired(): Unit = {
    val now = System.currentTimeMillis()
    val before = tokens.size()
    tokens.entrySet().removeIf(_.getValue.expiresAt <= now)
    val removedCnt = before - tokens.size()
    if (removedCnt > 0) info(s"Removed $removedCnt expired Connect tokens")
  }
}
