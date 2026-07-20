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

/**
 * Store for Spark Connect session tokens.
 * Each token is a random UUID mapped to an authenticated username and an expiry timestamp.
 */
trait SparkConnectTokenStore {

  /**
   * Creates a new token for the given username with a TTL.
   * @return (token, expiresAtMs)
   */
  def create(username: String): (String, Long)

  /**
   * Returns username for a valid, non-expired token, or None if missing / expired.
   */
  def getUser(token: String): Option[String]

  /**
   * Extends the TTL of an existing token.
   * @return new expiry time in milliseconds, or None if token was not found or expired
   */
  def renew(token: String): Option[Long]

  /** Invalidates a token. No-op if the token does not exist. */
  def revoke(token: String): Unit

  /** Releases resources (background threads, DB connections, etc.). */
  def stop(): Unit
}
