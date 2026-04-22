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

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.{MetadataUtils, StreamObserver}

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.grpc.SparkConnectSessionManager
import org.apache.kyuubi.service.NoopSparkConnectServer
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.shaded.spark.connect.proto._

class KyuubiSparkConnectServiceImplSuite extends KyuubiFunSuite {

  private val capturedContexts = new ConcurrentLinkedQueue[UserContext]()
  private val releasedSessions = new ConcurrentLinkedQueue[String]()
  private val openSessionCount = new AtomicInteger(0)
  @volatile private var failNextReleaseSession = false

  private var engineServer: Server = _
  private var engineChannel: ManagedChannel = _
  private var frontendServer: NoopSparkConnectServer = _
  private var clientChannel: ManagedChannel = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val engineName = InProcessServerBuilder.generateName()

    engineServer = InProcessServerBuilder
      .forName(engineName)
      .directExecutor()
      .addService(new SparkConnectServiceGrpc.SparkConnectServiceImplBase {

        override def executePlan(
            request: ExecutePlanRequest,
            responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
          capturedContexts.add(request.getUserContext)
          responseObserver.onNext(ExecutePlanResponse.getDefaultInstance)
          responseObserver.onNext(ExecutePlanResponse.getDefaultInstance)
          responseObserver.onNext(ExecutePlanResponse.getDefaultInstance)
          responseObserver.onCompleted()
        }

        override def analyzePlan(
            request: AnalyzePlanRequest,
            responseObserver: StreamObserver[AnalyzePlanResponse]): Unit = {
          capturedContexts.add(request.getUserContext)
          responseObserver.onNext(AnalyzePlanResponse.getDefaultInstance)
          responseObserver.onCompleted()
        }

        override def config(
            request: ConfigRequest,
            responseObserver: StreamObserver[ConfigResponse]): Unit = {
          capturedContexts.add(request.getUserContext)
          responseObserver.onNext(ConfigResponse.getDefaultInstance)
          responseObserver.onCompleted()
        }

        override def artifactStatus(
            request: ArtifactStatusesRequest,
            responseObserver: StreamObserver[ArtifactStatusesResponse]): Unit = {
          capturedContexts.add(request.getUserContext)
          responseObserver.onNext(ArtifactStatusesResponse.getDefaultInstance)
          responseObserver.onCompleted()
        }

        override def interrupt(
            request: InterruptRequest,
            responseObserver: StreamObserver[InterruptResponse]): Unit = {
          capturedContexts.add(request.getUserContext)
          responseObserver.onNext(InterruptResponse.getDefaultInstance)
          responseObserver.onCompleted()
        }

        override def reattachExecute(
            request: ReattachExecuteRequest,
            responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
          capturedContexts.add(request.getUserContext)
          responseObserver.onNext(ExecutePlanResponse.getDefaultInstance)
          responseObserver.onNext(ExecutePlanResponse.getDefaultInstance)
          responseObserver.onNext(ExecutePlanResponse.getDefaultInstance)
          responseObserver.onCompleted()
        }

        override def releaseExecute(
            request: ReleaseExecuteRequest,
            responseObserver: StreamObserver[ReleaseExecuteResponse]): Unit = {
          capturedContexts.add(request.getUserContext)
          responseObserver.onNext(ReleaseExecuteResponse.getDefaultInstance)
          responseObserver.onCompleted()
        }

        override def addArtifacts(
            responseObserver: StreamObserver[AddArtifactsResponse])
            : StreamObserver[AddArtifactsRequest] =
          new StreamObserver[AddArtifactsRequest] {
            override def onNext(req: AddArtifactsRequest): Unit =
              capturedContexts.add(req.getUserContext)
            override def onError(t: Throwable): Unit =
              responseObserver.onError(t)
            override def onCompleted(): Unit = {
              responseObserver.onNext(AddArtifactsResponse.getDefaultInstance)
              responseObserver.onCompleted()
            }
          }

        override def releaseSession(
            request: ReleaseSessionRequest,
            responseObserver: StreamObserver[ReleaseSessionResponse]): Unit = {
          if (failNextReleaseSession) {
            failNextReleaseSession = false
            responseObserver.onError(new RuntimeException("engine error"))
          } else {
            responseObserver.onNext(ReleaseSessionResponse.getDefaultInstance)
            responseObserver.onCompleted()
          }
        }
      })
      .build()
      .start()

    engineChannel = InProcessChannelBuilder
      .forName(engineName)
      .directExecutor()
      .build()

    frontendServer = new NoopSparkConnectServer {
      override val frontendServices = Seq(
        new KyuubiSparkConnectFrontendService(this) {
          override protected def createSessionManager(): SparkConnectSessionManager =
            new SparkConnectSessionManager(null) {
              private val fixedSession = ConnectSession(
                SessionHandle(java.util.UUID.randomUUID()),
                engineChannel,
                SparkConnectServiceGrpc.newStub(engineChannel))
              override def getOrOpen(sessionId: String, username: String): ConnectSession = {
                openSessionCount.incrementAndGet()
                fixedSession
              }
              override def get(sessionId: String): Option[ConnectSession] = Some(fixedSession)
              override def release(sessionId: String, closeKyuubiSession: Boolean = true): Unit =
                releasedSessions.add(sessionId)
              override def closeAll(): Unit = ()
            }
        })
    }

    frontendServer.initialize(
      KyuubiConf()
        .set(FRONTEND_SPARK_CONNECT_BIND_HOST.key, "localhost")
        .set(FRONTEND_SPARK_CONNECT_BIND_PORT, Utils.findFreePort())
        .set(AUTHENTICATION_METHOD, Seq("NOSASL")))
    frontendServer.start()

    val xUserHeader = new Metadata()
    xUserHeader.put(
      Metadata.Key.of("x-user-name", Metadata.ASCII_STRING_MARSHALLER),
      "john")
    clientChannel = ManagedChannelBuilder
      .forTarget(frontendServer.frontendServices.head.connectionUrl)
      .usePlaintext()
      .asInstanceOf[ManagedChannelBuilder[_]]
      .intercept(MetadataUtils.newAttachHeadersInterceptor(xUserHeader))
      .asInstanceOf[ManagedChannelBuilder[_]]
      .build()
  }

  override def afterAll(): Unit = {
    if (clientChannel != null) clientChannel.shutdownNow()
    if (frontendServer != null) frontendServer.stop()
    if (engineChannel != null) engineChannel.shutdownNow()
    if (engineServer != null) engineServer.shutdownNow()
    super.afterAll()
  }

  private class TestStreamObserver[R] extends StreamObserver[R] {
    private val _values = new java.util.ArrayList[R]()
    private val latch = new CountDownLatch(1)
    var error: Throwable = null
    override def onNext(v: R): Unit = _values.add(v)
    override def onError(t: Throwable): Unit = { error = t; latch.countDown() }
    override def onCompleted(): Unit = latch.countDown()
    def awaitCompletion(timeout: Long, unit: TimeUnit): Boolean = latch.await(timeout, unit)
    def values: java.util.List[R] = _values
  }

  private def asyncStub: SparkConnectServiceGrpc.SparkConnectServiceStub =
    SparkConnectServiceGrpc.newStub(clientChannel)

  private def evilContext: UserContext =
    UserContext.newBuilder().setUserId("evil").build()

  /** Invokes f with a StreamObserver and blocks until the call completes. Fails on error. */
  private def call[R](f: StreamObserver[R] => Unit): Unit = {
    val latch = new CountDownLatch(1)
    var callError: Throwable = null
    f(new StreamObserver[R] {
      override def onNext(v: R): Unit = ()
      override def onError(t: Throwable): Unit = { callError = t; latch.countDown() }
      override def onCompleted(): Unit = latch.countDown()
    })
    assert(latch.await(5, TimeUnit.SECONDS), "timed out waiting for gRPC response")
    assert(callError === null, s"unexpected error: $callError")
  }

  // UserContext rewriting

  test("executePlan: rewrites UserContext to authenticated user") {
    capturedContexts.clear()
    call[ExecutePlanResponse](asyncStub.executePlan(
      ExecutePlanRequest.newBuilder().setSessionId("s1").setUserContext(evilContext).build(),
      _))
    assert(capturedContexts.peek().getUserId === "john")
  }

  test("analyzePlan: rewrites UserContext to authenticated user") {
    capturedContexts.clear()
    call[AnalyzePlanResponse](asyncStub.analyzePlan(
      AnalyzePlanRequest.newBuilder().setSessionId("s1").setUserContext(evilContext).build(),
      _))
    assert(capturedContexts.peek().getUserId === "john")
  }

  test("config: rewrites UserContext to authenticated user") {
    capturedContexts.clear()
    call[ConfigResponse](asyncStub.config(
      ConfigRequest.newBuilder().setSessionId("s1").setUserContext(evilContext).build(),
      _))
    assert(capturedContexts.peek().getUserId === "john")
  }

  test("artifactStatus: rewrites UserContext to authenticated user") {
    capturedContexts.clear()
    call[ArtifactStatusesResponse](asyncStub.artifactStatus(
      ArtifactStatusesRequest.newBuilder().setSessionId("s1").setUserContext(evilContext).build(),
      _))
    assert(capturedContexts.peek().getUserId === "john")
  }

  test("interrupt: rewrites UserContext to authenticated user") {
    capturedContexts.clear()
    call[InterruptResponse](asyncStub.interrupt(
      InterruptRequest.newBuilder().setSessionId("s1").setUserContext(evilContext).build(),
      _))
    assert(capturedContexts.peek().getUserId === "john")
  }

  test("reattachExecute: rewrites UserContext to authenticated user") {
    capturedContexts.clear()
    call[ExecutePlanResponse](asyncStub.reattachExecute(
      ReattachExecuteRequest.newBuilder().setSessionId("s1").setUserContext(evilContext).build(),
      _))
    assert(capturedContexts.peek().getUserId === "john")
  }

  test("releaseExecute: rewrites UserContext to authenticated user") {
    capturedContexts.clear()
    call[ReleaseExecuteResponse](asyncStub.releaseExecute(
      ReleaseExecuteRequest.newBuilder().setSessionId("s1").setUserContext(evilContext).build(),
      _))
    assert(capturedContexts.peek().getUserId === "john")
  }

  // Server-streaming

  test("executePlan: client receives all streamed responses") {
    val recorder = new TestStreamObserver[ExecutePlanResponse]()
    asyncStub.executePlan(
      ExecutePlanRequest.newBuilder().setSessionId("s1").build(),
      recorder)
    assert(recorder.awaitCompletion(5, TimeUnit.SECONDS))
    assert(recorder.error === null)
    assert(recorder.values.size === 3)
  }

  test("reattachExecute: client receives all streamed responses") {
    val recorder = new TestStreamObserver[ExecutePlanResponse]()
    asyncStub.reattachExecute(
      ReattachExecuteRequest.newBuilder().setSessionId("s1").build(),
      recorder)
    assert(recorder.awaitCompletion(5, TimeUnit.SECONDS))
    assert(recorder.error === null)
    assert(recorder.values.size === 3)
  }

  // Bidirectional streaming

  test("addArtifacts: rewrites UserContext on each request") {
    capturedContexts.clear()
    val recorder = new TestStreamObserver[AddArtifactsResponse]()
    val requestObserver = asyncStub.addArtifacts(recorder)
    val request = AddArtifactsRequest.newBuilder()
      .setSessionId("s1")
      .setUserContext(evilContext)
      .build()
    requestObserver.onNext(request)
    requestObserver.onNext(request)
    requestObserver.onCompleted()
    assert(recorder.awaitCompletion(5, TimeUnit.SECONDS))
    assert(recorder.error === null)
    assert(capturedContexts.size === 2)
    capturedContexts.forEach(ctx => assert(ctx.getUserId === "john"))
  }

  test("addArtifacts: session not opened if no messages sent") {
    openSessionCount.set(0)
    val recorder = new TestStreamObserver[AddArtifactsResponse]()
    val requestObserver = asyncStub.addArtifacts(recorder)
    requestObserver.onCompleted()
    recorder.awaitCompletion(5, TimeUnit.SECONDS)
    assert(openSessionCount.get() === 0)
  }

  // Session release

  test("releaseSession: releases session on onCompleted") {
    releasedSessions.clear()
    call[ReleaseSessionResponse](asyncStub.releaseSession(
      ReleaseSessionRequest.newBuilder().setSessionId("s-complete").build(),
      _))
    assert(releasedSessions.contains("s-complete"))
  }

  test("releaseSession: releases session on onError") {
    releasedSessions.clear()
    failNextReleaseSession = true
    val latch = new CountDownLatch(1)
    asyncStub.releaseSession(
      ReleaseSessionRequest.newBuilder().setSessionId("s-error").build(),
      new StreamObserver[ReleaseSessionResponse] {
        override def onNext(v: ReleaseSessionResponse): Unit = ()
        override def onError(t: Throwable): Unit = latch.countDown()
        override def onCompleted(): Unit = latch.countDown()
      })
    assert(latch.await(5, TimeUnit.SECONDS), "timed out waiting for releaseSession error")
    assert(releasedSessions.contains("s-error"))
  }
}
