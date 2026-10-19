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

package org.apache.kyuubi.it.yarn

import java.nio.file.{Files, Paths}
import java.sql.{DriverManager, SQLException, Statement}
import java.util.UUID

import scala.concurrent.duration._
import scala.util.control.NonFatal

import org.apache.hadoop.security.alias.CredentialProviderFactory

import org.apache.kyuubi.{BoundedCall, KeyStoreTestUtils, KYUUBI_VERSION, KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_ID, KYUUBI_ENGINE_TRANSPORT_SSL}
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_NAMESPACE, HA_ZK_ENGINE_SECURE_SECRET_NODE}
import org.apache.kyuubi.ha.client.{DiscoveryPaths, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.it.yarn.KerberizedYarnCluster._

/**
 * Engine TLS on the kerberized YARN cluster of [[KerberizedYarnCluster]]. Spark SQL engines run
 * in YARN cluster mode on the NodeManager, as the session user that the Kyuubi server
 * impersonates with its Kerberos credentials. The engine key store exists on the NodeManager
 * only, and its password only in a JCEKS credential store on HDFS that the engine users may
 * read, so neither the Kyuubi servers nor the engine command line carry it. The Kyuubi servers
 * read the password of their trust store from the same credential store.
 *
 * Two Kyuubi servers share the cluster: one serves the regular cases, the other has a key store
 * path that exists on the Kyuubi server host but not on the NodeManager.
 *
 * It needs Docker and the build outputs listed in [[KyuubiBuild.locate]].
 */
class EngineSslOnKerberizedYarnSuite extends KyuubiFunSuite {

  private val tlsPort = 10109
  private val misconfiguredPort = 10110
  private val tlsNamespace = "kyuubi-it-engine-tls"
  private val misconfiguredNamespace = "kyuubi-it-engine-tls-misconfigured"

  // the members of engine-users may read the credential store, bob may not
  private val engineUsers = "engine-users"
  private val userGroups = Map(
    "kyuubi" -> Seq(engineUsers),
    "alice" -> Seq(engineUsers),
    "bob" -> Seq("users"),
    "carol" -> Seq(engineUsers))

  private val sslDir = "/etc/security/kyuubi"
  private val engineKeyStore = s"$sslDir/engine.p12"
  private val kyuubiHostOnlyKeyStore = s"$sslDir/kyuubi-host-only.p12"
  private val trustStore = s"$sslDir/truststore.p12"
  private val caPem = s"$sslDir/ca.pem"
  private val credentialStore = "/kyuubi/engine-ssl.jceks"

  private val keyStorePassword = s"engine-key-store-${UUID.randomUUID()}"
  private var trustStorePassword: String = _

  private var cluster: KerberizedYarnCluster = _
  private var tlsServer: ClusterContainer = _
  private var misconfiguredServer: ClusterContainer = _

  override def beforeAll(): Unit = {
    Class.forName("org.apache.kyuubi.jdbc.KyuubiHiveDriver")
    val build = KyuubiBuild.locate()
    val dir = Utils.createDirectory(System.getProperty("java.io.tmpdir"), "kerberized-yarn")
    val material = Files.createDirectories(dir.resolve("material"))
    val ca = KeyStoreTestUtils.generateCa(material)
    val keyStore = KeyStoreTestUtils.generateSignedKeyStore(
      material,
      ca,
      alias = "engine",
      cn = HOST,
      sans = Seq(s"dns:$HOST", "ip:127.0.0.1"),
      password = keyStorePassword)
    val trust = KeyStoreTestUtils.generateTrustStore(material, ca)
    trustStorePassword = KeyStoreTestUtils.passwordOf(trust)
    val credentials = Paths.get(KeyStoreTestUtils.createCredentialProvider(
      material,
      ENGINE_SSL_KEYSTORE_PASSWORD.key -> keyStorePassword,
      ENGINE_SSL_TRUSTSTORE_PASSWORD.key -> trustStorePassword).stripPrefix("jceks://file"))

    cluster = new KerberizedYarnCluster(
      dir,
      build,
      userGroups,
      nodeManagerFiles = Map(engineKeyStore -> keyStore),
      kyuubiPorts = Seq(tlsPort, misconfiguredPort))
    try {
      cluster.start()
      cluster.copyFile(cluster.nameNode, credentials, "/tmp/engine-ssl.jceks")
      cluster.asAdmin(
        cluster.nameNode,
        Seq(
          "hdfs dfs -mkdir -p /kyuubi",
          "hdfs dfs -chmod 755 /kyuubi",
          s"hdfs dfs -put -f /tmp/engine-ssl.jceks $credentialStore",
          s"hdfs dfs -chgrp $engineUsers $credentialStore",
          s"hdfs dfs -chmod 640 $credentialStore").mkString(" && "))
      tlsServer = cluster.startKyuubiServer(
        "kyuubi-tls",
        tlsPort,
        serverConf(tlsNamespace, engineKeyStore),
        Map(trustStore -> trust, caPem -> KeyStoreTestUtils.exportPem(ca)))
      misconfiguredServer = cluster.startKyuubiServer(
        "kyuubi-misconfigured",
        misconfiguredPort,
        serverConf(misconfiguredNamespace, kyuubiHostOnlyKeyStore),
        Map(trustStore -> trust, kyuubiHostOnlyKeyStore -> keyStore))
    } catch {
      case NonFatal(e) =>
        cluster.stop()
        throw e
    }
    // after the containers, so that the thread audit skips the long-living Testcontainers threads
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try super.afterAll()
    finally {
      if (cluster != null) {
        cluster.stop()
      }
    }
  }

  private def serverConf(namespace: String, keyStorePath: String): Map[String, String] = Map(
    HA_NAMESPACE.key -> namespace,
    ENGINE_SECURITY_ENABLED.key -> "true",
    ENGINE_SECURITY_SECRET_PROVIDER.key -> "zookeeper",
    HA_ZK_ENGINE_SECURE_SECRET_NODE.key -> "/kyuubi-it/engine-secret",
    ENGINE_ALIVE_PROBE_ENABLED.key -> "true",
    ENGINE_SSL_ENABLED.key -> "true",
    ENGINE_SSL_KEYSTORE_PATH.key -> keyStorePath,
    ENGINE_SSL_KEYSTORE_TYPE.key -> "PKCS12",
    ENGINE_SSL_TRUSTSTORE_PATH.key -> trustStore,
    ENGINE_SSL_TRUSTSTORE_TYPE.key -> "PKCS12",
    CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH ->
      s"jceks://hdfs@$HOST:$NAME_NODE_PORT$credentialStore")

  // an engine launch in YARN cluster mode takes minutes on an emulated amd64 image
  private def withStatement[T](port: Int, user: String)(f: Statement => T): T =
    BoundedCall.within(15.minutes) {
      val connection = DriverManager.getConnection(s"jdbc:hive2://$HOST:$port/default", user, "")
      try {
        val statement = connection.createStatement()
        try f(statement)
        finally statement.close()
      } finally {
        connection.close()
      }
    }

  private def setting(statement: Statement, key: String): String = {
    val result = statement.executeQuery(s"SET $key")
    assert(result.next(), s"SET $key returned no row")
    result.getString(2)
  }

  private def engines(namespace: String, user: String): Seq[ServiceNodeInfo] = {
    val conf = KyuubiConf().set(HA_ADDRESSES, s"$HOST:$ZOOKEEPER_PORT")
    val space = DiscoveryPaths.makePath(
      s"${namespace}_${KYUUBI_VERSION}_USER_SPARK_SQL",
      user,
      "default")
    withDiscoveryClient(conf)(_.getServiceNodesInfo(space, silent = true))
  }

  private def onlyEngine(namespace: String, user: String): ServiceNodeInfo = {
    val nodes = engines(namespace, user)
    assert(nodes.size === 1, s"engines of $user in $namespace: $nodes")
    nodes.head
  }

  test("an engine in YARN cluster mode serves TLS with the certificate of its NodeManager host") {
    val appId = withStatement(tlsPort, "alice") { statement =>
      assert(setting(statement, "spark.master") === "yarn")
      assert(setting(statement, "spark.submit.deployMode") === "cluster")
      assert(setting(statement, s"spark.${ENGINE_SSL_ENABLED.key}") === "true")
      val count = statement.executeQuery("SELECT count(*) FROM range(0, 100000, 1, 4)")
      assert(count.next())
      assert(count.getLong(1) === 100000L)
      setting(statement, "spark.app.id")
    }

    // YARN runs the engine as the session user, on the delegation tokens Kyuubi obtained for it
    val report = cluster.yarnApplication(appId)
    assert(report.contains("User : alice"), report)
    assert(report.contains("State : RUNNING"), report)

    val engine = onlyEngine(tlsNamespace, "alice")
    assert(engine.attributes.get(KYUUBI_ENGINE_ID).contains(appId))
    assert(engine.attributes.get(KYUUBI_ENGINE_TRANSPORT_SSL).contains("true"))
    // the key store the engine loaded only exists on the NodeManager
    val loaded = cluster.filesContaining(
      cluster.nodeManager,
      s"Loaded the engine TLS key store $engineKeyStore",
      YARN_LOG_DIR)
    assert(loaded.exists(_.contains(appId)), loaded)

    // From the Kyuubi server host, the engine completes a TLS handshake with a certificate chain
    // that verifies against the cluster CA and matches the host the engine registered.
    val verifyHost =
      if (engine.host.forall(c => c.isDigit || c == '.')) s"-verify_ip ${engine.host}"
      else s"-verify_hostname ${engine.host}"
    val handshake = cluster.bash(
      tlsServer,
      s"openssl s_client -connect ${engine.instance} -servername ${engine.host} $verifyHost " +
        s"-CAfile $caPem -verify_return_error -brief < /dev/null")
    assert(handshake.contains("Verification: OK"), handshake)
  }

  test("a second session reuses the TLS engine, which reads and writes kerberized HDFS") {
    val table = "/user/alice/engine_tls_hdfs"
    val rows = 5000
    withStatement(tlsPort, "alice") { statement =>
      val engine = onlyEngine(tlsNamespace, "alice")
      assert(engine.attributes.get(KYUUBI_ENGINE_ID).contains(setting(statement, "spark.app.id")))
      statement.execute(
        s"CREATE TABLE engine_tls_hdfs (id BIGINT, payload STRING) USING parquet LOCATION '$table'")
      statement.execute(
        s"INSERT INTO engine_tls_hdfs SELECT id, repeat('x', 1024) FROM range(0, $rows)")
      // about 5 MB in fetches of 500 rows, each spanning many TLS records
      statement.setFetchSize(500)
      val result = statement.executeQuery("SELECT id, payload FROM engine_tls_hdfs ORDER BY id")
      var fetched = 0
      while (result.next()) {
        assert(result.getLong(1) === fetched)
        assert(result.getString(2).length === 1024)
        fetched += 1
      }
      assert(fetched === rows)
    }
    // written by the executors as the session user
    val listing = cluster.asAdmin(cluster.nameNode, s"hdfs dfs -ls $table")
    assert(listing.contains(" alice "), listing)
  }

  test("the key store password reaches neither YARN nor the logs") {
    // the engine of alice still runs, so its launch context and localized configuration are on
    // the NodeManager
    onlyEngine(tlsNamespace, "alice")
    val yarnDirs = Seq(YARN_LOCAL_DIR, YARN_LOG_DIR)
    val launchConf = cluster.filesContaining(
      cluster.nodeManager,
      ENGINE_SSL_KEYSTORE_PATH.key,
      yarnDirs: _*)
    assert(launchConf.exists(_.endsWith("__spark_conf__.properties")), launchConf)

    Seq(keyStorePassword, trustStorePassword).foreach { secret =>
      assert(cluster.filesContaining(cluster.nodeManager, secret, yarnDirs: _*) === Nil)
      assert(cluster.filesContaining(tlsServer, secret, KYUUBI_TMP) === Nil)
      Seq(tlsServer, cluster.nodeManager, cluster.resourceManager).foreach { container =>
        assert(!container.getLogs.contains(secret))
      }
    }
  }

  test("a session user who cannot read the credential store on HDFS gets no engine") {
    val error = intercept[SQLException] {
      withStatement(tlsPort, "bob")(_.executeQuery("SELECT 1"))
    }
    assert(engines(tlsNamespace, "bob").isEmpty)
    // Spark itself reads the credential providers as bob, in the submission or in the
    // application master, and fails on the permissions of the credential store
    val denied = "Permission denied: user=bob"
    val launchLog = cluster.bash(tlsServer, s"cat $KYUUBI_TMP/work/bob/* 2>/dev/null || true")
    val evidence = Utils.stringifyException(error) + launchLog
    assert(
      evidence.contains(denied) ||
        cluster.filesContaining(cluster.nodeManager, denied, YARN_LOG_DIR).nonEmpty,
      evidence)
  }

  test("an engine on a NodeManager without the key store fails instead of serving plaintext") {
    // the key store exists on the Kyuubi server host, but Kyuubi never ships it to the engine
    cluster.bash(misconfiguredServer, s"test -r $kyuubiHostOnlyKeyStore")
    intercept[SQLException] {
      withStatement(misconfiguredPort, "carol")(_.executeQuery("SELECT 1"))
    }
    assert(engines(misconfiguredNamespace, "carol").isEmpty)
    val finished = cluster.yarnApplications("FAILED", "KILLED")
    assert(finished.split("\n").exists(_.contains("carol")), finished)
    val reasons = cluster.filesContaining(
      cluster.nodeManager,
      "the key store must exist on every NodeManager",
      YARN_LOG_DIR)
    assert(reasons.nonEmpty)
  }
}
