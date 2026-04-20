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

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.{KyuubiSessionImpl, SessionHandle}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.shaded.spark.connect.proto.SparkConnectServiceGrpc

class SparkConnectSessionManager(backendService: BackendService) extends Logging {

  protected def buildChannel(connectUrl: String): ManagedChannel =
    ManagedChannelBuilder
      .forTarget(connectUrl)
      .usePlaintext()
      .asInstanceOf[ManagedChannelBuilder[_]]
      .build()

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
        Map.empty)
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

  def closeAll(): Unit = {
    sessions.keys().asScala.toSeq.foreach(release(_))
  }
}
