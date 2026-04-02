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

import java.io.FileInputStream
import java.net.InetSocketAddress
import java.security.KeyStore
import javax.net.ssl.KeyManagerFactory

import io.grpc.{Server, ServerInterceptors}
import io.grpc.netty.{GrpcSslContexts, NettyServerBuilder}
import io.grpc.stub.StreamObserver
import io.netty.handler.ssl.SslContextBuilder

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.grpc.{BasicCredentialHandler, KerberosCredentialHandler, SparkConnectAuthInterceptor, SparkConnectAuthServiceImpl, SparkConnectCredentialHandler, SparkConnectKerberosValidator, SparkConnectRawHeaderContext, SparkConnectSessionManager, SparkConnectTokenStore}
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, AuthMethods, AuthTypes, AuthUtils}
import org.apache.kyuubi.shaded.spark.connect.proto._
import org.apache.kyuubi.util.JavaUtils

class KyuubiSparkConnectFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiSparkConnectFrontendService") {

  private var grpcServer: Server = _
  private var connectSessionManager: SparkConnectSessionManager = _
  private var authInterceptor: SparkConnectAuthInterceptor = _
  private var tokenStore: Option[SparkConnectTokenStore] = None
  private var authService: Option[SparkConnectAuthServiceImpl] = None

  private lazy val host: String = conf.get(FRONTEND_SPARK_CONNECT_BIND_HOST)
    .getOrElse {
      if (conf.get(KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME)) {
        JavaUtils.findLocalInetAddress.getCanonicalHostName
      } else {
        JavaUtils.findLocalInetAddress.getHostAddress
      }
    }

  private def userContext(user: String): UserContext =
    UserContext.newBuilder().setUserId(user).build()

  private lazy val serviceImpl = new SparkConnectServiceGrpc.SparkConnectServiceImplBase {

    override def executePlan(
        request: ExecutePlanRequest,
        responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.executePlan(
        request.toBuilder.setUserContext(userContext(user)).build(),
        responseObserver)
    }

    override def analyzePlan(
        request: AnalyzePlanRequest,
        responseObserver: StreamObserver[AnalyzePlanResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.analyzePlan(
        request.toBuilder.setUserContext(userContext(user)).build(),
        responseObserver)
    }

    override def config(
        request: ConfigRequest,
        responseObserver: StreamObserver[ConfigResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.config(
        request.toBuilder.setUserContext(userContext(user)).build(),
        responseObserver)
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
          upstreamObserver.onNext(req.toBuilder.setUserContext(userContext(user)).build())
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
      session.stub.artifactStatus(
        request.toBuilder.setUserContext(userContext(user)).build(),
        responseObserver)
    }

    override def interrupt(
        request: InterruptRequest,
        responseObserver: StreamObserver[InterruptResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.interrupt(
        request.toBuilder.setUserContext(userContext(user)).build(),
        responseObserver)
    }

    override def reattachExecute(
        request: ReattachExecuteRequest,
        responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.reattachExecute(
        request.toBuilder.setUserContext(userContext(user)).build(),
        responseObserver)
    }

    override def releaseExecute(
        request: ReleaseExecuteRequest,
        responseObserver: StreamObserver[ReleaseExecuteResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val session = connectSessionManager.getOrOpen(request.getSessionId, user, "", "")
      session.stub.releaseExecute(
        request.toBuilder.setUserContext(userContext(user)).build(),
        responseObserver)
    }

    override def releaseSession(
        request: ReleaseSessionRequest,
        responseObserver: StreamObserver[ReleaseSessionResponse]): Unit = {
      val user = SparkConnectAuthInterceptor.USER_KEY.get()
      val token = Option(SparkConnectAuthInterceptor.TOKEN_KEY.get())
      val sessionId = request.getSessionId
      val session = connectSessionManager.getOrOpen(sessionId, user, "", "")
      session.stub.releaseSession(
        request,
        new StreamObserver[ReleaseSessionResponse] {
          override def onNext(value: ReleaseSessionResponse): Unit = responseObserver.onNext(value)
          override def onError(t: Throwable): Unit = {
            connectSessionManager.release(sessionId)
            token.foreach(t => tokenStore.foreach(_.revoke(t)))
            responseObserver.onError(t)
          }
          override def onCompleted(): Unit = {
            connectSessionManager.release(sessionId)
            token.foreach(t => tokenStore.foreach(_.revoke(t)))
            responseObserver.onCompleted()
          }
        })
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
    connectSessionManager = new SparkConnectSessionManager(serverable.backendService)
    val authTypes = conf.get(KyuubiConf.AUTHENTICATION_METHOD)
      .map[AuthTypes.AuthType](AuthTypes.withName)

    val kerberosHandler: Option[SparkConnectCredentialHandler] = {
      if (AuthUtils.kerberosEnabled(authTypes) &&
        conf.get(SERVER_SPNEGO_KEYTAB).nonEmpty &&
        conf.get(SERVER_SPNEGO_PRINCIPAL).nonEmpty) {
        Some(new KerberosCredentialHandler(new SparkConnectKerberosValidator(conf)))
      } else {
        None
      }
    }

    val ldapHandler: Option[SparkConnectCredentialHandler] =
      AuthUtils.effectivePlainAuthType(authTypes).map { authType =>
        val method = AuthMethods.withName(authType.toString)
        new BasicCredentialHandler(
          AuthenticationProviderFactory.getAuthenticationProvider(method, conf, isServer = true))
      }

    val handlers: Seq[SparkConnectCredentialHandler] = Seq(kerberosHandler, ldapHandler).flatten
    if (handlers.nonEmpty) {
      val store = new SparkConnectTokenStore(conf.get(FRONTEND_SPARK_CONNECT_TOKEN_TTL))
      tokenStore = Some(store)
      authService = Some(new SparkConnectAuthServiceImpl(handlers, store))
      info("Spark Connect token auth service initialized")
    }
    authInterceptor = new SparkConnectAuthInterceptor(conf, tokenStore)
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    val port = conf.get(FRONTEND_SPARK_CONNECT_BIND_PORT)
    val boundService = ServerInterceptors.intercept(serviceImpl, authInterceptor)
    val builder = NettyServerBuilder
      .forAddress(new InetSocketAddress(host, port))
      .addService(boundService)

    // auth service is not behind authInterceptor
    // it does its own SPNEGO check internally
    if (authService.nonEmpty) {
      builder.addService(
        ServerInterceptors.intercept(authService.get, SparkConnectRawHeaderContext.interceptor))
    }

    if (conf.get(FRONTEND_SPARK_CONNECT_SSL_ENABLED)) {
      val keyStorePath = conf.get(FRONTEND_SSL_KEYSTORE_PATH)
      val keyStorePassword = conf.get(FRONTEND_SSL_KEYSTORE_PASSWORD)
      val keyStoreType = conf.get(FRONTEND_SSL_KEYSTORE_TYPE).getOrElse(KeyStore.getDefaultType)
      val keyStoreAlgorithm = conf.get(FRONTEND_SSL_KEYSTORE_ALGORITHM)
        .getOrElse(KeyManagerFactory.getDefaultAlgorithm)

      if (keyStorePath.isEmpty) {
        throw new IllegalArgumentException(
          s"${FRONTEND_SSL_KEYSTORE_PATH.key} not configured for SSL connection")
      }

      if (keyStorePassword.isEmpty) {
        throw new IllegalArgumentException(
          s"${FRONTEND_SSL_KEYSTORE_PASSWORD.key} not configured for SSL connection")
      }

      val keyStore = KeyStore.getInstance(keyStoreType)

      val fis = new FileInputStream(keyStorePath.get)
      try {
        keyStore.load(fis, keyStorePassword.get.toCharArray)
      } finally {
        fis.close()
      }
      val keyManagerFactory = KeyManagerFactory.getInstance(keyStoreAlgorithm)
      keyManagerFactory.init(keyStore, keyStorePassword.get.toCharArray)
      val sslContext =
        GrpcSslContexts.configure(SslContextBuilder.forServer(keyManagerFactory)).build()

      builder.sslContext(sslContext)
      info(s"SparkConnect frontend SSL enabled (keystore: ${keyStorePath.get})")
    }

    grpcServer = builder.build().start()
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
    tokenStore.foreach(_.stop())
    super.stop()
  }

  override def connectionUrl: String = s"$host:${grpcServer.getPort}"

  override val discoveryService: Option[Service] = None
}
