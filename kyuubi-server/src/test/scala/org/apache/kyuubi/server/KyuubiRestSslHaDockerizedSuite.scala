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

package org.apache.kyuubi.server

import java.io.File
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.dimafeng.testcontainers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.RestSslTestHelper._
import org.apache.kyuubi.client.{BatchRestApi, KyuubiRestClient}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_ZK_AUTH_TYPE}
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.session.{KyuubiSessionManager, SessionType}
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

/**
 * Two Kyuubi instances with SSL enabled for the RESTful frontend service share a PostgreSQL
 * metadata store running in a Docker container. Batch requests received by one instance are
 * redirected over https to the instance that owns the batch, verifying its certificate with
 * the truststore of the internal rest client.
 */
class KyuubiRestSslHaDockerizedSuite extends KyuubiFunSuite {

  private val postgresImage = sys.env.getOrElse("KYUUBI_IT_POSTGRES_IMAGE", "postgres:16.1")
  private val postgresPort = 5432
  private val postgresDb = "kyuubi_metadata"
  private val postgresUser = "kyuubi"
  private val postgresPassword = "kyuubi"

  private val postgres = GenericContainer(
    dockerImage = postgresImage,
    exposedPorts = Seq(postgresPort),
    env = Map(
      "POSTGRES_DB" -> postgresDb,
      "POSTGRES_USER" -> postgresUser,
      "POSTGRES_PASSWORD" -> postgresPassword),
    waitStrategy = Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2))

  private var zkServer: EmbeddedZookeeper = _
  private var servers: Seq[KyuubiServer] = Seq.empty
  private var trustStore: File = _

  override def beforeAll(): Unit = {
    postgres.start()
    // after the container, so that the thread audit skips the long-living Testcontainers
    // threads and covers the threads of the kyuubi services
    super.beforeAll()

    val keyStore = generateKeyStore(Utils.createTempDir().toFile)
    trustStore = generateTrustStore(keyStore)
    val conf = sslConf(KyuubiConf(), keyStore)
      .set(BATCH_INTERNAL_REST_CLIENT_SSL_TRUSTSTORE_PATH, trustStore.getAbsolutePath)
      .set(BATCH_INTERNAL_REST_CLIENT_SSL_TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD)
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.REST.toString))
      .set(FRONTEND_REST_BIND_HOST.key, "localhost")
      .set(FRONTEND_REST_BIND_PORT, 0)
      .set(AUTHENTICATION_METHOD, Seq("NONE"))
      .set(BATCH_IMPL_VERSION, "2")
      .set(BATCH_SUBMITTER_ENABLED, true)
      .set(METADATA_REQUEST_ASYNC_RETRY_ENABLED, false)
      .set(METADATA_STORE_JDBC_DATABASE_TYPE, "POSTGRESQL")
      .set(
        METADATA_STORE_JDBC_URL,
        s"jdbc:postgresql://${postgres.host}:${postgres.mappedPort(postgresPort)}/$postgresDb")
      .set(METADATA_STORE_JDBC_USER, postgresUser)
      .set(METADATA_STORE_JDBC_PASSWORD, postgresPassword)
      .set("kyuubi.metrics.enabled", "false")

    zkServer = new EmbeddedZookeeper()
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    conf.set(ZookeeperConf.ZK_DATA_DIR, Utils.createTempDir().toString)
    zkServer.initialize(conf)
    zkServer.start()
    conf.set(HA_ADDRESSES, zkServer.getConnectString)
    conf.set(HA_ZK_AUTH_TYPE, AuthTypes.NONE.toString)

    val started = ArrayBuffer.empty[KyuubiServer]
    try {
      (0 until 2).foreach { i =>
        started += KyuubiServer.startServer(conf.clone.set(SERVER_NAME, s"kyuubi-rest-ssl-ha-$i"))
      }
      servers = started.toSeq
    } catch {
      case NonFatal(e) =>
        started.foreach(stopQuietly)
        throw e
    }
  }

  override def afterAll(): Unit = {
    servers.foreach(stopQuietly)
    servers = Seq.empty
    if (zkServer != null) {
      zkServer.stop()
      zkServer = null
    }
    Utils.tryLogNonFatalError(postgres.stop())
    super.afterAll()
  }

  private def stopQuietly(server: KyuubiServer): Unit = {
    Utils.tryLogNonFatalError(server.stop())
  }

  private def restFrontend(server: KyuubiServer): KyuubiRestFrontendService =
    server.frontendServices.collectFirst { case fe: KyuubiRestFrontendService => fe }.get

  private def withBatchRestApi[T](fe: KyuubiRestFrontendService)(f: BatchRestApi => T): T = {
    val client = KyuubiRestClient.builder(s"${fe.connectionScheme}://${fe.connectionUrl}")
      .apiVersion(KyuubiRestClient.ApiVersion.V1)
      .username("anonymous")
      .password("anonymous")
      .maxAttempts(1)
      .trustStorePath(trustStore.getAbsolutePath)
      .trustStorePassword(KEYSTORE_PASSWORD)
      .build()
    try {
      f(new BatchRestApi(client))
    } finally {
      client.close()
    }
  }

  test("all kyuubi instances serve the REST API over https") {
    servers.map(restFrontend).foreach { fe =>
      assert(fe.connectionScheme === "https")
      val conn = httpsConnection(fe.connectionUrl, "/api/v1/ping")
      assert(conn.getResponseCode === 200)
      assert(readBody(conn) === "pong")
    }
  }

  test("batch request is redirected to the owner kyuubi instance over https") {
    val Seq(owner, other) = servers.map(restFrontend)
    val sessionManager = owner.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val batchId = UUID.randomUUID().toString
    sessionManager.insertMetadata(Metadata(
      identifier = batchId,
      sessionType = SessionType.BATCH,
      realUser = "anonymous",
      username = "anonymous",
      ipAddress = "localhost",
      kyuubiInstance = owner.connectionUrl,
      state = "PENDING",
      resource = "resource",
      className = "className",
      requestName = "rest-ssl-ha",
      engineType = "SPARK",
      createTime = System.currentTimeMillis()))
    try {
      // the log is produced by the owner instance only, the other instance has to redirect
      // the request to the owner via the internal rest client
      val expectedLog = Seq(s"Batch $batchId is waiting for submitting")
      Seq(owner, other).foreach { fe =>
        val log = withBatchRestApi(fe)(_.getBatchLocalLog(batchId, 0, 10))
        assert(log.getLogRowSet.asScala === expectedLog)
      }
    } finally {
      sessionManager.cleanupMetadata(batchId)
    }
  }
}
