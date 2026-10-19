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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.grpc.{ChannelCredentials, ManagedChannel, ManagedChannelBuilder, TlsChannelCredentials}
import io.grpc.netty.NettyChannelBuilder

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_PROFILE, ENGINE_SECURITY_ENABLED, ENGINE_SSL_ENABLED, FRONTEND_PROTOCOLS, FrontendProtocols, SESSION_CLOSE_ON_DISCONNECT}
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.ShareLevel.ShareLevel
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ENGINE_REF_ID
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, SessionHandle}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.shaded.spark.connect.proto.SparkConnectServiceGrpc

class SparkConnectSessionManager(
    backendService: BackendService,
    shareLevel: ShareLevel = ShareLevel.USER) extends Logging {

  // Reuses the trust material the KyuubiSessionManager loaded and checked at startup for the
  // Thrift connections, so both channels to an engine trust the same certificates. Lazy, so it
  // is only resolved when a channel is built.
  private lazy val engineTls: Option[(ChannelCredentials, Option[String])] = {
    val sessionManager = backendService.sessionManager
    if (sessionManager.getConf.get(ENGINE_SSL_ENABLED)) {
      val ctx = sessionManager match {
        case kyuubiSessionManager: KyuubiSessionManager =>
          kyuubiSessionManager.engineSslClientContext
        case other => throw new KyuubiException(
            s"${ENGINE_SSL_ENABLED.key} requires a KyuubiSessionManager, but the session " +
              s"manager is ${other.getClass.getName}")
      }
      val credentials = TlsChannelCredentials.newBuilder().trustManager(ctx.trustManagers: _*)
        .build()
      Some((credentials, ctx.expectedHostname))
    } else {
      None
    }
  }

  protected def buildChannel(connectUrl: String): ManagedChannel = {
    val builder: ManagedChannelBuilder[_] = engineTls match {
      case Some((credentials, expectedHostname)) =>
        // There is no plaintext fallback. grpc-netty verifies the engine certificate against
        // the channel authority, the host of connectUrl unless expectedHostname replaces it.
        val tlsBuilder = NettyChannelBuilder.forTarget(connectUrl, credentials)
        expectedHostname.foreach(tlsBuilder.overrideAuthority)
        tlsBuilder
      case None => ManagedChannelBuilder.forTarget(connectUrl).usePlaintext()
    }
    if (backendService.sessionManager.getConf.get(ENGINE_SECURITY_ENABLED)) {
      builder.intercept(new KyuubiEngineTokenClientInterceptor)
    }
    builder.build()
  }

  case class ConnectSession(
      kyuubiHandle: SessionHandle,
      channel: ManagedChannel,
      stub: SparkConnectServiceGrpc.SparkConnectServiceStub)

  private val sessions = new ConcurrentHashMap[String, ConnectSession]()

  def getOrOpen(
      sessionId: String,
      username: String): ConnectSession = {
    Option(sessions.get(sessionId)).getOrElse {
      val handle = backendService.openSession(
        TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
        username,
        "",
        "",
        Map(FRONTEND_PROTOCOLS.key -> FrontendProtocols.SPARK_CONNECT.toString) ++
          (if (shareLevel == ShareLevel.CONNECTION) Map(
             HA_ENGINE_REF_ID.key -> sessionId,
             SESSION_CLOSE_ON_DISCONNECT.key -> "false")
           else Map.empty) ++
          // Client-selected engine profile, e.g. sc://host:port/;kyuubi.engine.profile=spark42,
          // forwarded from the kyuubi.engine.profile gRPC header by SparkConnectAuthInterceptor.
          Option(SparkConnectAuthInterceptor.ENGINE_PROFILE_KEY.get())
            .map(ENGINE_PROFILE.key -> _).toMap)
      try {
        val kyuubiSession = backendService.sessionManager.getSession(handle) match {
          case s: KyuubiSessionImpl => s
          case other => throw new KyuubiException(
              s"Unexpected session type for Connect session $sessionId: ${other.getClass.getName}")
        }
        kyuubiSession.waitForEngineLaunched()
        val connectUrl = kyuubiSession.engineConnectUrl.getOrElse(
          throw new KyuubiException(
            s"Engine for Connect session $sessionId did not register a Connect URL"))
        val channel = buildChannel(connectUrl)
        val session = ConnectSession(handle, channel, SparkConnectServiceGrpc.newStub(channel))
        val existing = sessions.putIfAbsent(sessionId, session)
        if (existing != null) {
          channel.shutdownNow()
          backendService.closeSession(handle)
          existing
        } else session
      } catch {
        case NonFatal(e) =>
          backendService.closeSession(handle)
          throw e
      }
    }
  }

  def get(sessionId: String): Option[ConnectSession] = Option(sessions.get(sessionId))

  // Client-initiated disconnect: sends Thrift CloseSession so the engine stops immediately.
  def release(sessionId: String, closeKyuubiSession: Boolean = true): Unit = {
    Option(sessions.remove(sessionId)).foreach { s =>
      try {
        s.channel.shutdownNow()
      } catch {
        case NonFatal(e) => warn(s"Error shutting down channel for session $sessionId: $e")
      }
      if (closeKyuubiSession) {
        try {
          backendService.closeSession(s.kyuubiHandle)
        } catch {
          case NonFatal(e) =>
            warn(s"Error closing Kyuubi session for Connect session $sessionId: $e")
        }
      }
    }
  }

  // Server shutdown: for CONNECTION share level, skip Thrift CloseSession so the engine gets
  // a grace period (TCP drop triggers handleDisconnect) and can survive for HA failover.
  def closeAll(): Unit = {
    sessions.keys().asScala.toSeq.foreach { sessionId =>
      Option(sessions.remove(sessionId)).foreach { s =>
        try {
          s.channel.shutdownNow()
        } catch {
          case NonFatal(e) => warn(s"Error shutting down channel for session $sessionId: $e")
        }
        if (shareLevel != ShareLevel.CONNECTION) {
          try {
            backendService.closeSession(s.kyuubiHandle)
          } catch {
            case NonFatal(e) =>
              warn(s"Error closing Kyuubi session for Connect session $sessionId: $e")
          }
        }
      }
    }
  }
}
