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

import io.grpc.stub.StreamObserver

import org.apache.kyuubi.Logging
import org.apache.kyuubi.service.AbstractBackendService
import org.apache.kyuubi.session.{GrpcSessionHandle, KyuubiGrpcSessionManager, KyuubiSessionManager}
import org.apache.kyuubi.shaded.spark.connect.proto

class KyuubiGrpcBackendService extends AbstractBackendService("KyuubiGrpcBackendService")
  with proto.SparkConnectServiceGrpc.AsyncService with BackendServiceMetric with Logging {

  override val sessionManager: KyuubiSessionManager = new KyuubiSessionManager()
  val grpcSessionManager: KyuubiGrpcSessionManager = new KyuubiGrpcSessionManager()

  override def initialize(conf: org.apache.kyuubi.config.KyuubiConf): Unit = {
    addService(grpcSessionManager)
    super.initialize(conf)
  }

  // Wraps a StreamObserver to silently absorb errors with CANCELLED status that occur when
  // the client disconnects or cancels the request before the engine responds.
  private def safeObserver[T](observer: StreamObserver[T]): StreamObserver[T] =
    new StreamObserver[T] {
      override def onNext(value: T): Unit = observer.onNext(value)
      override def onError(t: Throwable): Unit = {
        val status = io.grpc.Status.fromThrowable(t)
        if (status.getCode == io.grpc.Status.Code.CANCELLED) {
          warn(s"Ignoring error forwarding on CANCELLED call: $t")
        } else {
          observer.onError(t)
        }
      }

      override def onCompleted(): Unit = observer.onCompleted()
    }

  override def executePlan(
      req: proto.ExecutePlanRequest,
      respObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    info(s"executePlan - session_id: ${req.getSessionId}, operation_id: ${req.getOperationId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = grpcSessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.executePlan(req, safeObserver(respObserver))
  }

  override def analyzePlan(
      req: proto.AnalyzePlanRequest,
      respObserver: StreamObserver[proto.AnalyzePlanResponse]): Unit = {
    warn(s"analyzePlan - session_id: ${req.getSessionId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = grpcSessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.analyzePlan(req, safeObserver(respObserver))
  }

  override def config(
      req: proto.ConfigRequest,
      respObserver: StreamObserver[proto.ConfigResponse]): Unit = {
    warn(s"config - session_id: ${req.getSessionId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = grpcSessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.config(req, safeObserver(respObserver))
  }

  override def addArtifacts(respObserver: StreamObserver[proto.AddArtifactsResponse])
      : StreamObserver[proto.AddArtifactsRequest] = {
    info(s"addArtifacts")
    val safe = safeObserver(respObserver)
    new StreamObserver[proto.AddArtifactsRequest] {

      private var sparkRequestObserver: StreamObserver[proto.AddArtifactsRequest] = _

      override def onNext(req: proto.AddArtifactsRequest): Unit = {
        if (sparkRequestObserver == null) {
          val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
            Some(req.getClientObservedServerSideSessionId)
          } else None
          val session = grpcSessionManager.getOrCreateSession(
            new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
            previousSessionId)
          sparkRequestObserver = session.client.astub.addArtifacts(safe)
        }
        sparkRequestObserver.onNext(req)
      }

      override def onError(t: Throwable): Unit = {
        if (sparkRequestObserver != null) {
          sparkRequestObserver.onError(t)
        } else {
          safe.onError(t)
        }
      }

      override def onCompleted(): Unit = {
        sparkRequestObserver.onCompleted()
        /*
        if (sparkRequestObserver != null) {
          sparkRequestObserver.onCompleted()
        } else {
          safe.onCompleted()
        }
        */
      }
    }
  }

  override def artifactStatus(
      req: proto.ArtifactStatusesRequest,
      respObserver: StreamObserver[proto.ArtifactStatusesResponse]): Unit = {
    warn(s"artifactStatus - session_id: ${req.getSessionId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = grpcSessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.artifactStatus(req, safeObserver(respObserver))
  }

  override def interrupt(
      req: proto.InterruptRequest,
      respObserver: StreamObserver[proto.InterruptResponse]): Unit = {
    warn(s"interrupt - session_id: ${req.getSessionId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = grpcSessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.interrupt(req, safeObserver(respObserver))
  }

  override def reattachExecute(
      req: proto.ReattachExecuteRequest,
      respObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    warn(s"reattachExecute - session_id: ${req.getSessionId}, operation_id: ${req.getOperationId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = grpcSessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.reattachExecute(req, safeObserver(respObserver))
  }

  override def releaseExecute(
      req: proto.ReleaseExecuteRequest,
      respObserver: StreamObserver[proto.ReleaseExecuteResponse]): Unit = {
    warn(s"releaseExecute - session_id: ${req.getSessionId}, operation_id: ${req.getOperationId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = grpcSessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.releaseExecute(req, safeObserver(respObserver))
  }

  override def releaseSession(
      req: proto.ReleaseSessionRequest,
      respObserver: StreamObserver[proto.ReleaseSessionResponse]): Unit = {
    warn(s"releaseSession - session_id: ${req.getSessionId}")
    val session = grpcSessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      None)
    session.client.astub.releaseSession(req, safeObserver(respObserver))
  }

  override def fetchErrorDetails(
      req: proto.FetchErrorDetailsRequest,
      respObserver: StreamObserver[proto.FetchErrorDetailsResponse]): Unit = {
    warn(s"fetchErrorDetails - session_id: ${req.getSessionId}, error_id: ${req.getErrorId}")
    val previousSessionId = if (req.hasClientObservedServerSideSessionId) {
      Some(req.getClientObservedServerSideSessionId)
    } else None
    val session = grpcSessionManager.getOrCreateSession(
      new GrpcSessionHandle(req.getUserContext.getUserId, req.getSessionId),
      previousSessionId)
    session.client.astub.fetchErrorDetails(req, safeObserver(respObserver))
  }
}
