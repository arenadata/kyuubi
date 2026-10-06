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

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_ZK_AUTH_TYPE}
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

trait WithKyuubiHaCluster extends KyuubiFunSuite {

  protected val conf: KyuubiConf

  protected val serverCount: Int = 2

  /** Prefix for per-node `kyuubi.server.name`, e.g. `kyuubi-sc-ha`. */
  protected def serverNamePrefix: String

  /** Set frontend protocol(s), bind host/port, and any protocol defaults. */
  protected def configureFrontends(conf: KyuubiConf): Unit

  private var zkServer: EmbeddedZookeeper = _
  protected var servers: Seq[KyuubiServer] = Seq.empty

  override def beforeAll(): Unit = {
    configureFrontends(conf)
    conf.set(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(FRONTEND_REST_BIND_PORT, 0)
    conf.set(FRONTEND_MYSQL_BIND_PORT, 0)
    conf.setIfMissing(ENGINE_SHARE_LEVEL, "connection")
    conf.setIfMissing("kyuubi.metrics.enabled", "false")
    conf.set("spark.ui.enabled", "false")
    conf.setIfMissing("spark.sql.catalogImplementation", "in-memory")
    conf.setIfMissing("kyuubi.ha.zookeeper.connection.retry.policy", "ONE_TIME")
    conf.setIfMissing(ENGINE_CHECK_INTERVAL, 1000L)
    conf.setIfMissing(ENGINE_IDLE_TIMEOUT, 60000L)

    zkServer = new EmbeddedZookeeper()
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    val zkData = Utils.createTempDir()
    conf.set(ZookeeperConf.ZK_DATA_DIR, zkData.toString)
    zkServer.initialize(conf)
    zkServer.start()
    conf.set(HA_ADDRESSES, zkServer.getConnectString)
    conf.set(HA_ZK_AUTH_TYPE, AuthTypes.NONE.toString)

    val started = ArrayBuffer.empty[KyuubiServer]
    try {
      (0 until serverCount).foreach { i =>
        val serverConf = conf.clone
          .set(SERVER_NAME, s"$serverNamePrefix-$i")
        started += KyuubiServer.startServer(serverConf)
      }
      servers = started.toSeq
    } catch {
      case NonFatal(e) =>
        started.foreach(stopQuietly)
        if (zkServer != null) {
          zkServer.stop()
          zkServer = null
        }
        throw e
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    servers.foreach { server =>
      closeSessionsQuietly(server)
      stopQuietly(server)
    }
    servers = Seq.empty

    if (zkServer != null) {
      zkServer.stop()
      zkServer = null
    }
    super.afterAll()
  }

  protected def zkAddresses: String = conf.get(HA_ADDRESSES)

  protected def peerOf(server: KyuubiServer): KyuubiServer =
    servers.find(_ ne server).getOrElse(
      throw new IllegalStateException("expected at least one peer Kyuubi server"))

  protected def findServerWithSessions: KyuubiServer = {
    servers.find(s => s != null && s.backendService.sessionManager.allSessions().nonEmpty)
      .getOrElse(throw new IllegalStateException("No Kyuubi server has an open session"))
  }

  private def closeSessionsQuietly(server: KyuubiServer): Unit = {
    if (server == null) return
    try {
      val sessionManager =
        server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
      sessionManager.allSessions().foreach { session =>
        try {
          sessionManager.closeSession(session.handle)
        } catch {
          case NonFatal(e) =>
            logger.warn(s"Error closing session ${session.handle}", e)
        }
      }
    } catch {
      case NonFatal(e) =>
        logger.warn("Error while closing sessions before server stop", e)
    }
  }

  private def stopQuietly(server: KyuubiServer): Unit = {
    if (server == null) return
    try {
      server.stop()
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error stopping server ${server.getName}", e)
    }
  }
}
