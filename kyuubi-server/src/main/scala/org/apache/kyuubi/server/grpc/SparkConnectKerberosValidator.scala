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

import java.io.File
import java.security.{MessageDigest, PrivilegedActionException, PrivilegedExceptionAction}
import java.util.Base64
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import javax.security.auth.Subject
import javax.security.auth.kerberos.{KerberosPrincipal, KeyTab}
import javax.security.sasl.AuthenticationException

import org.apache.hadoop.security.authentication.util.KerberosName
import org.apache.hadoop.security.authentication.util.KerberosUtil._
import org.ietf.jgss.{GSSContext, GSSCredential, GSSManager, Oid}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{SERVER_SPNEGO_KEYTAB, SERVER_SPNEGO_PRINCIPAL}

/**
 * Validates SPNEGO tokens for the Spark Connect gRPC frontend.
 * Reuses the same GSS-API pattern as KerberosAuthenticationHandler but adapted for gRPC
 */
class SparkConnectKerberosValidator(conf: KyuubiConf) extends Logging {

  private val keytab = conf.get(SERVER_SPNEGO_KEYTAB).get
  private val principal = conf.get(SERVER_SPNEGO_PRINCIPAL).get

  private val serverSubject: Subject = {
    val subject = new Subject()
    subject.getPrivateCredentials.add(KeyTab.getInstance(new File(keytab)))
    subject.getPrincipals.add(new KerberosPrincipal(principal))
    subject
  }

  private val gssManager: GSSManager = Subject.doAs(
    serverSubject,
    new PrivilegedExceptionAction[GSSManager] {
      override def run(): GSSManager = GSSManager.getInstance()
    })

  if (!KerberosName.hasRulesBeenSet) {
    KerberosName.setRules("DEFAULT")
  }

  // Cache: SHA-256(token) -> (user, expireMillis)
  private val tokenCacheTtlMs = 30_000L
  private val tokenCache = new ConcurrentHashMap[String, (String, Long)]()
  private val cacheCleanupExecutor = {
    val executor = Executors.newSingleThreadScheduledExecutor(r => {
      val t = new Thread(r, "spnego-token-cache-cleanup")
      t.setDaemon(true)
      t
    })
    executor.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit =
          tokenCache.entrySet().removeIf(e => e.getValue._2 < System.currentTimeMillis())
      },
      60, 60, TimeUnit.SECONDS)
    executor
  }

  info(s"SparkConnectKerberosValidator initialized with principal $principal, keytab $keytab")

  /**
   * Validates a SPNEGO token from the Authorization: Negotiate header.
   *
   * @param base64Token the base64-encoded SPNEGO token (with "Negotiate " prefix already stripped)
   * @return the authenticated short username (e.g. "user" from "user@REALM.COM")
   * @throws AuthenticationException if token validation fails
   */
  def validate(base64Token: String): String = {
    val clientToken = Base64.getDecoder.decode(base64Token)
    try {
      Subject.doAs(
        serverSubject,
        new PrivilegedExceptionAction[String] {
          override def run(): String = validateToken(clientToken)
        })
    } catch {
      case e: PrivilegedActionException =>
        throw new AuthenticationException("SPNEGO authentication failed", e.getException)
      case e: Exception =>
        throw new AuthenticationException("SPNEGO authentication failed", e)
    }
  }

  def close(): Unit = cacheCleanupExecutor.shutdownNow()

  private def tokenHash(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x".format(_)).mkString

  private def validateToken(clientToken: Array[Byte]): String = {
    val hash = tokenHash(clientToken)

    // we can get duplicate token (concurrent streams can use same metadata)
    val cached = tokenCache.get(hash)
    if (cached != null && cached._2 > System.currentTimeMillis()) {
      debug(s"SPNEGO token cache hit hash=$hash user=${cached._1}")
      return cached._1
    }

    val serverPrincipalName = getTokenServerName(clientToken)
    var gssContext: GSSContext = null
    var gssCreds: GSSCredential = null
    try {
      debug(s"SPNEGO validating token hash=$hash server principal=$serverPrincipalName")
      gssCreds = gssManager.createCredential(
        gssManager.createName(serverPrincipalName, NT_GSS_KRB5_PRINCIPAL_OID),
        GSSCredential.INDEFINITE_LIFETIME,
        Array[Oid](GSS_SPNEGO_MECH_OID, GSS_KRB5_MECH_OID),
        GSSCredential.ACCEPT_ONLY)
      gssContext = gssManager.createContext(gssCreds)
      gssContext.acceptSecContext(clientToken, 0, clientToken.length)
      if (!gssContext.isEstablished) {
        throw new AuthenticationException("SPNEGO context wasn't fully established")
      }
      val clientPrincipal = gssContext.getSrcName.toString
      val shortName = new KerberosName(clientPrincipal).getShortName
      debug(s"SPNEGO completed for client principal $clientPrincipal:" +
        s" $shortName, caching hash=$hash")
      tokenCache.put(hash, (shortName, System.currentTimeMillis() + tokenCacheTtlMs))
      shortName
    } finally {
      if (gssContext != null) gssContext.dispose()
      if (gssCreds != null) gssCreds.dispose()
    }
  }
}
