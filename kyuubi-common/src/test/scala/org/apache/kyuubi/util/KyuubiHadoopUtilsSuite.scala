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

import java.io.{DataInput, DataOutput}
import java.nio.file.{Files, Paths}
import java.util.stream.StreamSupport

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier
import org.apache.hadoop.hdfs.security.token.delegation.{DelegationTokenIdentifier => HDFSTokenIdent}
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_SSL_KEYSTORE_PASSWORD, FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PASSWORD, HADOOP_CREDENTIAL_CACHE_TTL}
import org.apache.kyuubi.service.authentication.KyuubiDelegationTokenIdentifier

class KyuubiHadoopUtilsSuite extends KyuubiFunSuite {

  test("new hadoop conf with kyuubi conf") {
    val abc = "hadoop.abc"
    val xyz = "hadoop.xyz"
    val test = "hadoop.test"
    val kyuubiConf = new KyuubiConf()
      .set(abc, "xyz")
      .set(xyz, "abc")
      .set(test, "t")
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    assert(hadoopConf.get(abc) === "xyz")
    assert(hadoopConf.get(xyz) === "abc")
    assert(hadoopConf.get(test) === "t")
  }

  test("encode/decode credentials") {
    val identifier = new KyuubiDelegationTokenIdentifier()
    val password = new Array[Byte](128)
    Random.nextBytes(password)
    val token = new Token[KyuubiDelegationTokenIdentifier](
      identifier.getBytes,
      password,
      identifier.getKind,
      new Text(""))
    val credentials = new Credentials()
    credentials.addToken(token.getKind, token)

    val decoded = KyuubiHadoopUtils.decodeCredentials(
      KyuubiHadoopUtils.encodeCredentials(credentials))
    assert(decoded.getToken(token.getKind) == credentials.getToken(token.getKind))
  }

  test("new hadoop conf with kyuubi conf with loadDefaults") {
    val abc = "kyuubi.abc"
    val kyuubiConf = new KyuubiConf()
      .set(abc, "xyz")

    var hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    assert(StreamSupport.stream(hadoopConf.spliterator(), false)
      .anyMatch(entry => entry.getKey.startsWith("hadoop") || entry.getKey.startsWith("fs")))

    hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf, loadDefaults = false)
    assert(StreamSupport.stream(hadoopConf.spliterator(), false)
      .noneMatch(entry => entry.getKey.startsWith("hadoop") || entry.getKey.startsWith("fs")))
  }

  test("get password from hadoop credential provider") {
    val providerPath =
      s"jceks://file${Utils.createTempDir().toAbsolutePath}/test.jceks"
    val providerConf = new Configuration(false)
    providerConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    val provider = CredentialProviderFactory.getProviders(providerConf).get(0)
    provider.createCredentialEntry(
      FRONTEND_SSL_KEYSTORE_PASSWORD.key,
      "provider-password".toCharArray)
    provider.flush()

    // no alias, no literal value
    val emptyConf = KyuubiConf(loadSysDefault = false)
    assert(KyuubiHadoopUtils.getPassword(emptyConf, FRONTEND_SSL_KEYSTORE_PASSWORD).isEmpty)

    // no alias, fall back to the literal config value
    val literalConf = KyuubiConf(loadSysDefault = false)
      .set(FRONTEND_SSL_KEYSTORE_PASSWORD, "literal-password")
    assert(KyuubiHadoopUtils.getPassword(literalConf, FRONTEND_SSL_KEYSTORE_PASSWORD)
      .contains("literal-password"))

    // alias resolved from the credential provider, overriding the literal value
    val providerKyuubiConf = KyuubiConf(loadSysDefault = false)
      .set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
      .set(FRONTEND_SSL_KEYSTORE_PASSWORD, "literal-password")
    assert(KyuubiHadoopUtils.getPassword(providerKyuubiConf, FRONTEND_SSL_KEYSTORE_PASSWORD)
      .contains("provider-password"))

    // alias stored under an alternative key is resolved too
    assert(
      KyuubiHadoopUtils.getPassword(providerKyuubiConf, FRONTEND_THRIFT_HTTP_SSL_KEYSTORE_PASSWORD)
        .contains("provider-password"))

    // literal values are passed through verbatim, no Hadoop variable substitution
    val substConf = KyuubiConf(loadSysDefault = false)
      .set(FRONTEND_SSL_KEYSTORE_PASSWORD, "p${user.name}w")
    assert(KyuubiHadoopUtils.getPassword(substConf, FRONTEND_SSL_KEYSTORE_PASSWORD)
      .contains("p${user.name}w"))

    // provider errors fall back to the literal value instead of failing
    val brokenConf = KyuubiConf(loadSysDefault = false)
      .set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, "unknown://broken/path")
      .set(FRONTEND_SSL_KEYSTORE_PASSWORD, "literal-password")
    assert(KyuubiHadoopUtils.getPassword(brokenConf, FRONTEND_SSL_KEYSTORE_PASSWORD)
      .contains("literal-password"))
  }

  test("credential provider alias cache ttl") {
    val providerPath = s"jceks://file${Utils.createTempDir().toAbsolutePath}/ttl.jceks"
    val providerConf = new Configuration(false)
    providerConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    val provider = CredentialProviderFactory.getProviders(providerConf).get(0)
    provider.createCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key, "v1".toCharArray)
    provider.flush()

    val conf = KyuubiConf(loadSysDefault = false)
      .set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    assert(KyuubiHadoopUtils.getPassword(conf, FRONTEND_SSL_KEYSTORE_PASSWORD).contains("v1"))

    provider.deleteCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key)
    provider.createCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key, "v2".toCharArray)
    provider.flush()

    // within the default ttl the rotated value is not visible yet
    assert(KyuubiHadoopUtils.getPassword(conf, FRONTEND_SSL_KEYSTORE_PASSWORD).contains("v1"))

    // ttl 0 disables caching, rotation is picked up immediately
    conf.set(HADOOP_CREDENTIAL_CACHE_TTL.key, "0")
    assert(KyuubiHadoopUtils.getPassword(conf, FRONTEND_SSL_KEYSTORE_PASSWORD).contains("v2"))
  }

  test("rotated alias is picked up after the cached entry expires") {
    val providerPath = s"jceks://file${Utils.createTempDir().toAbsolutePath}/rotate.jceks"
    val providerConf = new Configuration(false)
    providerConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    val provider = CredentialProviderFactory.getProviders(providerConf).get(0)
    provider.createCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key, "v1".toCharArray)
    provider.flush()

    // a short ttl so the cached entry expires between reads
    val conf = KyuubiConf(loadSysDefault = false)
      .set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
      .set(HADOOP_CREDENTIAL_CACHE_TTL.key, "1")
    assert(KyuubiHadoopUtils.getPassword(conf, FRONTEND_SSL_KEYSTORE_PASSWORD).contains("v1"))

    provider.deleteCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key)
    provider.createCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key, "v2".toCharArray)
    provider.flush()
    Thread.sleep(10)
    assert(KyuubiHadoopUtils.getPassword(conf, FRONTEND_SSL_KEYSTORE_PASSWORD).contains("v2"))
  }

  test("lowering the ttl retroactively expires an already cached entry") {
    val providerPath = s"jceks://file${Utils.createTempDir().toAbsolutePath}/lower.jceks"
    val providerConf = new Configuration(false)
    providerConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    val provider = CredentialProviderFactory.getProviders(providerConf).get(0)
    provider.createCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key, "v1".toCharArray)
    provider.flush()

    // cache under a long ttl, so a write-time expiry stamp would pin v1 for minutes
    val conf = KyuubiConf(loadSysDefault = false)
      .set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
      .set(HADOOP_CREDENTIAL_CACHE_TTL.key, "600000")
    assert(KyuubiHadoopUtils.getPassword(conf, FRONTEND_SSL_KEYSTORE_PASSWORD).contains("v1"))

    provider.deleteCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key)
    provider.createCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key, "v2".toCharArray)
    provider.flush()

    // lowering the ttl must shorten the existing entry's lifetime, not keep serving v1
    conf.set(HADOOP_CREDENTIAL_CACHE_TTL.key, "1")
    Thread.sleep(10)
    assert(KyuubiHadoopUtils.getPassword(conf, FRONTEND_SSL_KEYSTORE_PASSWORD).contains("v2"))
  }

  test("keep serving the resolved alias when the credential store fails") {
    val storeDir = Utils.createTempDir().toAbsolutePath
    val providerPath = s"jceks://file$storeDir/stale.jceks"
    val providerConf = new Configuration(false)
    providerConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    val provider = CredentialProviderFactory.getProviders(providerConf).get(0)
    provider.createCredentialEntry(FRONTEND_SSL_KEYSTORE_PASSWORD.key, "resolved".toCharArray)
    provider.flush()

    // expiry is stamped at write time, so the entry must be cached under the short ttl
    val conf = KyuubiConf(loadSysDefault = false)
      .set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
      .set(HADOOP_CREDENTIAL_CACHE_TTL.key, "1")
    assert(KyuubiHadoopUtils.getPassword(conf, FRONTEND_SSL_KEYSTORE_PASSWORD)
      .contains("resolved"))

    // the store becomes unusable and the cached entry expires
    Files.write(Paths.get(s"$storeDir/stale.jceks"), "corrupted".getBytes())
    Thread.sleep(10)

    // the expired refresh fails, and the last resolved value is served instead of None
    assert(KyuubiHadoopUtils.getPassword(conf, FRONTEND_SSL_KEYSTORE_PASSWORD)
      .contains("resolved"))
  }

  test("get token issue date") {
    val issueDate = System.currentTimeMillis()

    def checkIssueDate(tokenIdent: TokenIdentifier, expected: Option[Long]): Unit = {
      val hdfsToken = new Token[HDFSTokenIdent]()
      hdfsToken.setKind(tokenIdent.getKind)
      hdfsToken.setID(tokenIdent.getBytes)
      assert(KyuubiHadoopUtils.getTokenIssueDate(hdfsToken) == expected)
    }

    // DelegationTokenIdentifier found in ServiceLoader
    // Such as HDFS_DELEGATION_TOKEN, OzoneToken
    val hdfsTokenIdent = new HDFSTokenIdent()
    hdfsTokenIdent.setIssueDate(issueDate)
    checkIssueDate(hdfsTokenIdent, Some(issueDate))

    // TokenIdentifier with no issue date found in ServiceLoader
    val blockTokenIdent = new BlockTokenIdentifier()
    checkIssueDate(blockTokenIdent, None)

    // DelegationTokenIdentifier not found in ServiceLoader
    // Such as HIVE_DELEGATION_TOKEN
    val testTokenIdent = new TestDelegationTokenIdentifier()
    testTokenIdent.setIssueDate(issueDate)
    checkIssueDate(testTokenIdent, Some(issueDate))

    // DelegationTokenIdentifier with custom binary format and not found in ServiceLoader
    val testTokenIdent2 = new TestDelegationTokenIdentifier2()
    testTokenIdent2.setIssueDate(issueDate)
    checkIssueDate(testTokenIdent2, None)
  }
}

private class TestDelegationTokenIdentifier extends AbstractDelegationTokenIdentifier {
  override def getKind: Text = new Text("KYUUBI_TOKEN_NOT_IN_SERVICE_LOADER")
}

private class TestDelegationTokenIdentifier2 extends AbstractDelegationTokenIdentifier {
  override def getKind: Text = new Text("KYUUBI_TOKEN_OVERRIDE_WRITE")

  override def write(out: DataOutput): Unit = {
    out.writeLong(getIssueDate)
  }

  override def readFields(in: DataInput): Unit = {
    setIssueDate(in.readLong())
  }
}
