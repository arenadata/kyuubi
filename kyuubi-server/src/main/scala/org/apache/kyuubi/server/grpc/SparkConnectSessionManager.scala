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

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.{KyuubiSessionImpl, SessionHandle}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.shaded.spark.connect.proto.SparkConnectServiceGrpc

class SparkConnectSessionManager(be: BackendService) extends Logging {

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
      username: String,
      password: String,
      ipAddr: String): ConnectSession = {
    sessions.computeIfAbsent(
      sessionId,
      _ => {
        val handle = be.openSession(
          TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
          username,
          password,
          ipAddr,
          Map.empty)
        val kyuubiSession = be.sessionManager
          .getSession(handle)
          .asInstanceOf[KyuubiSessionImpl]
        kyuubiSession.waitForEngineLaunched()
        val connectUrl = kyuubiSession.engineConnectUrl.getOrElse(
          throw new KyuubiException(
            s"Engine for Connect session $sessionId did not register a Connect URL"))
        val channel = buildChannel(connectUrl)
        ConnectSession(handle, channel, SparkConnectServiceGrpc.newStub(channel))
      })
  }

  def release(sessionId: String): Unit = {
    Option(sessions.remove(sessionId)).foreach { s =>
      try {
        s.channel.shutdownNow()
      } catch {
        case e: Throwable => warn(s"Error shutting down channel for session $sessionId: $e")
      }
      try {
        be.closeSession(s.kyuubiHandle)
      } catch {
        case e: Throwable =>
          warn(s"Error closing Kyuubi session for Connect session $sessionId: $e")
      }
    }
  }

  def closeAll(): Unit = {
    sessions.keys().asScala.toSeq.foreach(release)
  }
}
