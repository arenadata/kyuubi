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

package org.apache.kyuubi

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.KeyStore
import java.security.cert.{Certificate, CertificateFactory}
import java.util.{Base64, Locale, UUID}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import javax.net.ssl.{KeyManager, KeyManagerFactory, TrustManager, TrustManagerFactory}

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.util.JavaUtils

/**
 * Generates TLS key material for tests at runtime, with the keytool of the running JDK and the
 * JDK KeyStore API only, so that every module consuming the kyuubi-common test-jar can use it.
 * Certificates are RSA 2048, valid from one day in the past to tolerate clock skew.
 *
 * Ready-made material is generated lazily, once per JVM: a test CA, key stores signed by it for
 * the good, wrong host and expired cases, a self-signed key store nobody trusts, and trust
 * stores holding the test CA only.
 */
object KeyStoreTestUtils {

  val PASSWORD = "kyuubi-engine-ssl-test"
  val CA_ALIAS = "ca"
  val DEFAULT_ALIAS = "engine"

  private case class StoreInfo(password: String, storeType: String)

  private val stores = new ConcurrentHashMap[Path, StoreInfo]()

  /**
   * Subject alternative names covering every name and address a local engine is reached by:
   * localhost, the loopback address, and the address and canonical host name that
   * [[JavaUtils.findLocalInetAddress]] picks for engines without a bind host.
   */
  lazy val defaultSans: Seq[String] = {
    val local = JavaUtils.findLocalInetAddress
    val dnsNames = Seq("localhost", local.getCanonicalHostName).filter(isDnsName)
    // keytool cannot parse an IPv6 scope id
    val ips = Seq("127.0.0.1", local.getHostAddress.takeWhile(_ != '%'))
    (dnsNames.map("dns:" + _) ++ ips.map("ip:" + _)).distinct
  }

  lazy val materialDir: Path = Utils.createTempDir("kyuubi-engine-ssl")

  lazy val caKeyStore: Path = generateCa(materialDir)

  /** Signed by the test CA, valid for [[defaultSans]]. */
  lazy val goodKeyStore: Path = generateSignedKeyStore(materialDir, caKeyStore)

  lazy val goodJksKeyStore: Path =
    generateSignedKeyStore(materialDir, caKeyStore, alias = "engine-jks", storeType = "JKS")

  /** Trusts the test CA only. */
  lazy val trustStore: Path = generateTrustStore(materialDir, caKeyStore)

  lazy val jksTrustStore: Path = generateTrustStore(materialDir, "JKS", caKeyStore)

  /** Self-signed, valid for [[defaultSans]], trusted by no trust store of this object. */
  lazy val foreignKeyStore: Path = generateKeyStore(materialDir, alias = "foreign")

  /** Signed by the test CA, only valid for other.invalid. */
  lazy val otherHostKeyStore: Path = generateSignedKeyStore(
    materialDir,
    caKeyStore,
    alias = "other-host",
    cn = "other.invalid",
    sans = Seq("dns:other.invalid"))

  /** Signed by the test CA, only valid for the DNS name localhost, not for 127.0.0.1. */
  lazy val localhostOnlyKeyStore: Path = generateSignedKeyStore(
    materialDir,
    caKeyStore,
    alias = "localhost-only",
    sans = Seq("dns:localhost"))

  /** Signed by the test CA, expired two days ago. */
  lazy val expiredKeyStore: Path = generateSignedKeyStore(
    materialDir,
    caKeyStore,
    alias = "expired",
    startDate = "-3d",
    validityDays = 1)

  /** A key store with a self-signed certificate. */
  def generateKeyStore(
      dir: Path,
      alias: String = DEFAULT_ALIAS,
      cn: String = "localhost",
      sans: Seq[String] = defaultSans,
      storeType: String = "PKCS12",
      password: String = PASSWORD,
      startDate: String = "-1d",
      validityDays: Int = 2): Path = {
    val store = newStorePath(dir, alias, storeType)
    keytool(genKeyPairArgs(store, alias, s"CN=$cn", storeType, password) ++ sanArgs(sans) ++
      Seq("-startdate", startDate, "-validity", validityDays.toString): _*)
    register(store, password, storeType)
  }

  /** A key store holding a CA key pair under [[CA_ALIAS]], to sign other key stores. */
  def generateCa(
      dir: Path,
      cn: String = "Kyuubi Engine SSL Test CA",
      storeType: String = "PKCS12",
      password: String = PASSWORD,
      validityDays: Int = 30): Path = {
    val store = newStorePath(dir, CA_ALIAS, storeType)
    keytool(genKeyPairArgs(store, CA_ALIAS, s"CN=$cn", storeType, password) ++
      Seq("-ext", "bc:c", "-startdate", "-1d", "-validity", validityDays.toString): _*)
    register(store, password, storeType)
  }

  /** A key store whose certificate is signed by `ca`, holding the chain up to the CA. */
  def generateSignedKeyStore(
      dir: Path,
      ca: Path,
      alias: String = DEFAULT_ALIAS,
      cn: String = "localhost",
      sans: Seq[String] = defaultSans,
      storeType: String = "PKCS12",
      password: String = PASSWORD,
      startDate: String = "-1d",
      validityDays: Int = 2): Path = {
    val store = newStorePath(dir, alias, storeType)
    keytool(genKeyPairArgs(store, alias, s"CN=$cn", storeType, password): _*)
    val csr = dir.resolve(s"${store.getFileName}.csr")
    val crt = dir.resolve(s"${store.getFileName}.crt")
    keytool(
      "-certreq",
      "-alias",
      alias,
      "-storetype",
      storeType,
      "-keystore",
      store.toString,
      "-storepass",
      password,
      "-file",
      csr.toString)
    val caInfo = info(ca)
    keytool(Seq(
      "-gencert",
      "-rfc",
      "-alias",
      CA_ALIAS,
      "-storetype",
      caInfo.storeType,
      "-keystore",
      ca.toString,
      "-storepass",
      caInfo.password,
      "-infile",
      csr.toString,
      "-outfile",
      crt.toString,
      "-startdate",
      startDate,
      "-validity",
      validityDays.toString) ++ sanArgs(sans): _*)

    // replace the self-signed placeholder with the chain [signed certificate, CA]
    val keyStore = load(store, StoreInfo(password, storeType))
    val signed = withInput(crt)(CertificateFactory.getInstance("X.509").generateCertificate)
    val chain = Array[Certificate](signed, load(ca, caInfo).getCertificate(CA_ALIAS))
    val key = keyStore.getKey(alias, password.toCharArray)
    keyStore.setKeyEntry(alias, key, password.toCharArray, chain)
    save(keyStore, store, password)
    Files.deleteIfExists(csr)
    Files.deleteIfExists(crt)
    register(store, password, storeType)
  }

  /** A PKCS12 trust store with the trust anchor of each given key store. */
  def generateTrustStore(dir: Path, certsFrom: Path*): Path =
    generateTrustStore(dir, "PKCS12", certsFrom: _*)

  /** A trust store of the given type with the trust anchor of each given key store. */
  def generateTrustStore(dir: Path, storeType: String, certsFrom: Path*): Path = {
    val store = newStorePath(dir, "truststore", storeType)
    val trusted = KeyStore.getInstance(storeType)
    trusted.load(null, null)
    certsFrom.zipWithIndex.foreach { case (source, i) =>
      trusted.setCertificateEntry(s"trusted-$i", trustAnchor(source))
    }
    save(trusted, store, PASSWORD)
    register(store, PASSWORD, storeType)
  }

  /**
   * The certificate a client has to trust to accept the given key store: the root of its key
   * entry's chain, which is the certificate itself when it is self-signed.
   */
  def trustAnchor(keyStore: Path): Certificate = {
    val store = loadKeyStore(keyStore)
    val aliases = store.aliases().asScala.toList
    aliases.find(store.isKeyEntry).map(a => store.getCertificateChain(a).last)
      .orElse(aliases.find(store.isCertificateEntry).map(store.getCertificate))
      .getOrElse(throw new IllegalArgumentException(s"$keyStore holds no certificate"))
  }

  /** Write the [[trustAnchor]] of the key store as a PEM file next to it. */
  def exportPem(keyStore: Path): Path = {
    val pem = keyStore.resolveSibling(s"${keyStore.getFileName}.pem")
    val body = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
      .encodeToString(trustAnchor(keyStore).getEncoded)
    val text = s"-----BEGIN CERTIFICATE-----\n$body\n-----END CERTIFICATE-----\n"
    Files.write(pem, text.getBytes(StandardCharsets.US_ASCII))
  }

  /**
   * A new JCEKS credential provider holding the given aliases. Every call uses a new file, so
   * the credential alias cache of KyuubiHadoopUtils never serves a value from another test.
   *
   * @return the value for hadoop.security.credential.provider.path
   */
  def createCredentialProvider(dir: Path, aliases: (String, String)*): String = {
    val file = dir.resolve(s"credentials-${UUID.randomUUID()}.jceks").toAbsolutePath
    val providerPath = s"jceks://file$file"
    val providerConf = new Configuration(false)
    providerConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
    val provider = CredentialProviderFactory.getProviders(providerConf).get(0)
    aliases.foreach { case (alias, secret) =>
      provider.createCredentialEntry(alias, secret.toCharArray)
    }
    provider.flush()
    providerPath
  }

  def passwordOf(store: Path): String = info(store).password

  def storeTypeOf(store: Path): String = info(store).storeType

  def loadKeyStore(store: Path): KeyStore = load(store, info(store))

  def keyManagers(keyStore: Path): Array[KeyManager] = {
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(loadKeyStore(keyStore), passwordOf(keyStore).toCharArray)
    kmf.getKeyManagers
  }

  def trustManagers(trustStore: Path): Array[TrustManager] = {
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(loadKeyStore(trustStore))
    tmf.getTrustManagers
  }

  /** Engine side settings serving TLS with the given key store, with a literal password. */
  def engineSslConf(keyStore: Path = goodKeyStore): Map[String, String] = Map(
    ENGINE_SSL_ENABLED.key -> "true",
    ENGINE_SSL_KEYSTORE_PATH.key -> keyStore.toString,
    ENGINE_SSL_KEYSTORE_PASSWORD.key -> passwordOf(keyStore),
    ENGINE_SSL_KEYSTORE_TYPE.key -> storeTypeOf(keyStore))

  /** Kyuubi server side settings verifying engine certificates with the given trust store. */
  def serverSslConf(trustStore: Path = this.trustStore): Map[String, String] = Map(
    ENGINE_SSL_ENABLED.key -> "true",
    ENGINE_SSL_TRUSTSTORE_PATH.key -> trustStore.toString,
    ENGINE_SSL_TRUSTSTORE_PASSWORD.key -> passwordOf(trustStore),
    ENGINE_SSL_TRUSTSTORE_TYPE.key -> storeTypeOf(trustStore))

  private def isDnsName(name: String): Boolean =
    name.exists(_.isLetter) &&
      name.forall(c => c.isLetterOrDigit || c == '-' || c == '.') &&
      name.split('.').forall(label => label.nonEmpty && label.head.isLetter)

  private def info(store: Path): StoreInfo = {
    val key = store.toAbsolutePath.normalize
    Option(stores.get(key)).getOrElse {
      val name = key.getFileName.toString.toLowerCase(Locale.ROOT)
      StoreInfo(PASSWORD, if (name.endsWith(".jks")) "JKS" else "PKCS12")
    }
  }

  private def register(store: Path, password: String, storeType: String): Path = {
    val key = store.toAbsolutePath.normalize
    stores.put(key, StoreInfo(password, storeType))
    key
  }

  private def newStorePath(dir: Path, alias: String, storeType: String): Path = {
    Files.createDirectories(dir)
    val ext = if (storeType.equalsIgnoreCase("JKS")) "jks" else "p12"
    dir.resolve(s"$alias-${UUID.randomUUID()}.$ext").toAbsolutePath
  }

  private def genKeyPairArgs(
      store: Path,
      alias: String,
      dname: String,
      storeType: String,
      password: String): Seq[String] = Seq(
    "-genkeypair",
    "-noprompt",
    "-alias",
    alias,
    "-keyalg",
    "RSA",
    "-keysize",
    "2048",
    "-dname",
    dname,
    "-storetype",
    storeType,
    "-keystore",
    store.toString,
    "-storepass",
    password,
    "-keypass",
    password)

  private def sanArgs(sans: Seq[String]): Seq[String] =
    if (sans.isEmpty) Nil else Seq("-ext", s"SAN=${sans.mkString(",")}")

  private def load(store: Path, info: StoreInfo): KeyStore = {
    val keyStore = KeyStore.getInstance(info.storeType)
    withInput(store)(in => keyStore.load(in, info.password.toCharArray))
    keyStore
  }

  private def save(keyStore: KeyStore, store: Path, password: String): Unit = {
    val out = Files.newOutputStream(store)
    try keyStore.store(out, password.toCharArray)
    finally out.close()
  }

  private def withInput[T](file: Path)(f: java.io.InputStream => T): T = {
    val in = Files.newInputStream(file)
    try f(in)
    finally in.close()
  }

  private def keytool(args: String*): Unit = {
    val keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool").toString
    val process = new ProcessBuilder((keytool +: args): _*).redirectErrorStream(true).start()
    // keytool must never wait for an answer
    process.getOutputStream.close()
    val output =
      try Source.fromInputStream(process.getInputStream, StandardCharsets.UTF_8.name()).mkString
      finally process.getInputStream.close()
    require(
      process.waitFor(60, TimeUnit.SECONDS) && process.exitValue() == 0,
      s"keytool ${args.head} failed: $output")
  }
}
