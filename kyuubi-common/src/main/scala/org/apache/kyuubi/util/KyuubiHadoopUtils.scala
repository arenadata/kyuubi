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

package org.apache.kyuubi.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.{Arrays, Base64, Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, SecurityUtil}
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{ConfigEntry, KyuubiConf, OptionalConfigEntry}
import org.apache.kyuubi.util.reflect.ReflectUtils._

object KyuubiHadoopUtils extends Logging {

  def newHadoopConf(
      conf: KyuubiConf,
      loadDefaults: Boolean = true): Configuration = {
    val hadoopConf = new Configuration(loadDefaults)
    conf.getAll
      .foreach { case (k, v) => hadoopConf.set(k, v) }
    hadoopConf
  }

  def newYarnConfiguration(conf: KyuubiConf): YarnConfiguration = {
    new YarnConfiguration(newHadoopConf(conf))
  }

  /** Credential provider path from the default Hadoop configuration (core-site.xml). */
  private lazy val defaultCredentialProviderPath: Option[String] =
    Option(new Configuration().getTrimmed(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH))

  private case class CachedAlias(value: Option[String], loadedAt: Long, failed: Boolean)

  /** Upper bound on how long a failed lookup is kept before the store is tried again. */
  private val FAILURE_RETRY_INTERVAL_MS = 30000L

  /**
   * Resolved aliases keyed by (provider path, alias). Freshness is evaluated at read time
   * against the current TTL, so lowering `kyuubi.hadoop.credential.cache.ttl` immediately
   * shortens the lifetime of already-cached entries. A failed lookup keeps serving the last
   * resolved value but is retried after at most `FAILURE_RETRY_INTERVAL_MS`.
   */
  private val credentialAliasCache = new ConcurrentHashMap[(String, String), CachedAlias]()

  private def aliasLifetimeMs(cached: CachedAlias, ttl: Long): Long =
    if (cached.failed) math.min(ttl, FAILURE_RETRY_INTERVAL_MS) else ttl

  /**
   * Resolve a password config entry. A credential provider alias named by the entry's key (or an
   * alternative key) takes precedence over the literal config value; provider errors are logged
   * and fall back to the literal. An alias-only value must be resolvable in the process that
   * reads it, including engines.
   */
  def getPassword(conf: KyuubiConf, entry: OptionalConfigEntry[String]): Option[String] = {
    resolvePassword(conf, entry.key :: entry.alternatives)
  }

  /** Variant for entries with a default value: falls back to the entry's default. */
  def getPassword(conf: KyuubiConf, entry: ConfigEntry[String]): String = {
    resolvePassword(conf, entry.key :: entry.alternatives).getOrElse(conf.get(entry))
  }

  private def resolvePassword(conf: KyuubiConf, keys: List[String]): Option[String] = {
    val providerPath = conf.getOption(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH)
      .orElse(defaultCredentialProviderPath)
    providerPath.flatMap { path =>
      keys.foldLeft(Option.empty[String]) { (res, key) =>
        res.orElse(resolveAlias(conf, path, key))
      }
    }.orElse {
      keys.foldLeft(Option.empty[String])((res, key) => res.orElse(conf.getOption(key)))
    }
  }

  private def resolveAlias(conf: KyuubiConf, path: String, key: String): Option[String] = {
    val ttl = conf.get(KyuubiConf.HADOOP_CREDENTIAL_CACHE_TTL)
    if (ttl <= 0) {
      // caching disabled: never read from or write to the cache, so no secret is retained
      // and no outage protection applies
      try lookupAlias(conf, path, key)
      catch {
        case e: Exception =>
          warn(s"Failed to resolve credential provider alias '$key' via '$path'", e)
          None
      }
    } else {
      val cacheKey = (path, key)
      val cached = credentialAliasCache.get(cacheKey)
      val now = System.currentTimeMillis()
      if (cached != null && now - cached.loadedAt < aliasLifetimeMs(cached, ttl)) {
        cached.value
      } else {
        // Looked up outside the cache lock: duplicate concurrent lookups are cheaper than
        // serializing every caller behind one store read.
        try {
          val value = lookupAlias(conf, path, key)
          credentialAliasCache.put(cacheKey, CachedAlias(value, System.currentTimeMillis(), false))
          value
        } catch {
          case e: Exception =>
            warn(s"Failed to resolve credential provider alias '$key' via '$path'", e)
            val at = System.currentTimeMillis()
            // Atomically: keep serving the last resolved value, but never overwrite an entry
            // another thread just resolved and that is still fresh.
            credentialAliasCache.compute(
              cacheKey,
              (_, cur) =>
                if (cur != null && at - cur.loadedAt < aliasLifetimeMs(cur, ttl)) {
                  cur
                } else {
                  CachedAlias(if (cur != null) cur.value else None, at, failed = true)
                }).value
        }
      }
    }
  }

  private def lookupAlias(conf: KyuubiConf, path: String, key: String): Option[String] = {
    val hadoopConf = newHadoopConf(conf)
    hadoopConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, path)
    Option(hadoopConf.getPasswordFromCredentialProviders(key)).map { chars =>
      val value = String.valueOf(chars)
      Arrays.fill(chars, 0.toChar)
      value
    }
  }

  def getServerPrincipal(principal: String): String = {
    SecurityUtil.getServerPrincipal(principal, "0.0.0.0")
  }

  def encodeCredentials(creds: Credentials): String = {
    val byteStream = new ByteArrayOutputStream
    creds.writeTokenStorageToStream(new DataOutputStream(byteStream))

    Base64.getEncoder.encodeToString(byteStream.toByteArray)
  }

  def decodeCredentials(newValue: String): Credentials = {
    val decoded = Base64.getDecoder.decode(newValue)

    val byteStream = new ByteArrayInputStream(decoded)
    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(byteStream))
    creds
  }

  def serializeCredentials(creds: Credentials): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    creds.writeTokenStorageToStream(dataStream)
    byteStream.toByteArray
  }

  def deserializeCredentials(tokenBytes: Array[Byte]): Credentials = {
    val tokensBuf = new ByteArrayInputStream(tokenBytes)

    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(tokensBuf))
    creds
  }

  /**
   * Get [[Credentials#tokenMap]] by reflection as [[Credentials#getTokenMap]] is not present before
   * Hadoop 3.2.1.
   */
  def getTokenMap(credentials: Credentials): Map[Text, Token[_ <: TokenIdentifier]] =
    getField[JMap[Text, Token[_ <: TokenIdentifier]]](credentials, "tokenMap").asScala.toMap

  def getTokenIssueDate(token: Token[_ <: TokenIdentifier]): Option[Long] = {
    token.decodeIdentifier() match {
      case tokenIdent: AbstractDelegationTokenIdentifier =>
        Some(tokenIdent.getIssueDate)
      case null =>
        // TokenIdentifiers not found in ServiceLoader
        val tokenIdentifier = new DelegationTokenIdentifier
        val buf = new ByteArrayInputStream(token.getIdentifier)
        val in = new DataInputStream(buf)
        Try(tokenIdentifier.readFields(in)) match {
          case Success(_) =>
            Some(tokenIdentifier.getIssueDate)
          case Failure(e) =>
            warn(s"Can not decode identifier of token $token", e)
            None
        }
      case tokenIdent =>
        debug(s"Unsupported TokenIdentifier kind: ${tokenIdent.getKind}")
        None
    }
  }

  def compareIssueDate(
      newToken: Token[_ <: TokenIdentifier],
      oldToken: Token[_ <: TokenIdentifier]): Int = {
    val newDate = KyuubiHadoopUtils.getTokenIssueDate(newToken)
    val oldDate = KyuubiHadoopUtils.getTokenIssueDate(oldToken)
    if (newDate.isDefined && oldDate.isDefined && newDate.get <= oldDate.get) {
      -1
    } else {
      1
    }
  }

  /**
   * Add a path variable to the given environment map.
   * If the map already contains this key, append the value to the existing value instead.
   */
  def addPathToEnvironment(env: HashMap[String, String], key: String, value: String): Unit = {
    val newValue =
      if (env.contains(key)) {
        env(key) + ApplicationConstants.CLASS_PATH_SEPARATOR + value
      } else {
        value
      }
    env.put(key, newValue)
  }
}
