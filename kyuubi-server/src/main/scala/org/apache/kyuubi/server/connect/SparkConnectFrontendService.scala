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

package org.apache.kyuubi.server.connect

import io.grpc.{Server, ServerInterceptors}
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.apache.kyuubi.shaded.spark.connect.proto._

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_CONNECT_BIND_HOST, FRONTEND_CONNECT_BIND_PORT}
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}
import org.apache.kyuubi.util.JavaUtils

class SparkConnectFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("SparkConnectFrontendService") {

  private var grpcServer: Server = _
  private var connectSessionManager: ConnectSessionManager = _
  private var authInterceptor: SparkConnectAuthInterceptor = _

  private lazy val host: String = conf.get(FRONTEND_CONNECT_BIND_HOST)
    .getOrElse {
      if (conf.get(KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME)) {
        JavaUtils.findLocalInetAddress.getCanonicalHostName
      } else {
        JavaUtils.findLocalInetAddress.getHostAddress
      }
    }

  private lazy val serviceImpl = new SparkConnectServiceGrpc.SparkConnectServiceImplBase {

    override def executePlan(
        request: ExecutePlanRequest,
        responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.executePlan(request, responseObserver)
    }

    override def analyzePlan(
        request: AnalyzePlanRequest,
        responseObserver: StreamObserver[AnalyzePlanResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.analyzePlan(request, responseObserver)
    }

    override def config(
        request: ConfigRequest,
        responseObserver: StreamObserver[ConfigResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.config(request, responseObserver)
    }

    override def addArtifacts(
        responseObserver: StreamObserver[AddArtifactsResponse])
        : StreamObserver[AddArtifactsRequest] = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      var upstreamObserver: StreamObserver[AddArtifactsRequest] = null
      new StreamObserver[AddArtifactsRequest] {
        override def onNext(req: AddArtifactsRequest): Unit = {
          if (upstreamObserver == null) {
            val session = connectSessionManager.getOrOpen(req.getSessionId, user, "", "")
            upstreamObserver = session.stub.addArtifacts(responseObserver)
          }
          upstreamObserver.onNext(req)
        }
        override def onError(t: Throwable): Unit =
          Option(upstreamObserver).foreach(_.onError(t))
        override def onCompleted(): Unit =
          Option(upstreamObserver).foreach(_.onCompleted())
      }
    }

    override def artifactStatus(
        request: ArtifactStatusesRequest,
        responseObserver: StreamObserver[ArtifactStatusesResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.artifactStatus(request, responseObserver)
    }

    override def interrupt(
        request: InterruptRequest,
        responseObserver: StreamObserver[InterruptResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.interrupt(request, responseObserver)
    }

    override def reattachExecute(
        request: ReattachExecuteRequest,
        responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.reattachExecute(request, responseObserver)
    }

    override def releaseExecute(
        request: ReleaseExecuteRequest,
        responseObserver: StreamObserver[ReleaseExecuteResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.releaseExecute(request, responseObserver)
    }

    override def releaseSession(
        request: ReleaseSessionRequest,
        responseObserver: StreamObserver[ReleaseSessionResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.releaseSession(request, responseObserver)
    }

    override def fetchErrorDetails(
        request: FetchErrorDetailsRequest,
        responseObserver: StreamObserver[FetchErrorDetailsResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.fetchErrorDetails(request, responseObserver)
    }
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    connectSessionManager = new ConnectSessionManager(serverable.backendService)
    authInterceptor = new SparkConnectAuthInterceptor(conf)
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    val port = conf.get(FRONTEND_CONNECT_BIND_PORT)
    val boundService = ServerInterceptors.intercept(serviceImpl, authInterceptor)
    grpcServer = NettyServerBuilder
      .forPort(port)
      .addService(boundService)
      .build()
      .start()
    info(s"SparkConnect frontend service started at $host:${grpcServer.getPort}")
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (grpcServer != null && !grpcServer.isShutdown) {
      grpcServer.shutdown()
    }
    if (connectSessionManager != null) {
      connectSessionManager.closeAll()
    }
    super.stop()
  }

  override def connectionUrl: String = s"$host:${grpcServer.getPort}"

  override val discoveryService: Option[Service] = None
}
