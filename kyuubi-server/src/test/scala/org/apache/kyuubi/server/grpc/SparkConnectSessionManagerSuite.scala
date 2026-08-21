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

import io.grpc.{Context, ManagedChannel}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, doThrow, times, verify, verifyNoInteractions, when}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf.ENGINE_PROFILE
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.{KyuubiSessionImpl, Session, SessionHandle}
import org.apache.kyuubi.session.SessionManager

class SparkConnectSessionManagerSuite extends KyuubiFunSuite with MockitoSugar {

  private def makeManager(
      backendService: BackendService,
      channel: ManagedChannel): SparkConnectSessionManager =
    new SparkConnectSessionManager(backendService) {
      override protected def buildChannel(connectUrl: String): ManagedChannel = channel
    }

  /** Open a session via getOrOpen with mocked dependencies. */
  private def openSession(
      manager: SparkConnectSessionManager,
      backendService: BackendService,
      sessionId: String): SessionHandle = {
    val handle = SessionHandle(java.util.UUID.randomUUID())
    val sessionMgr = mock[SessionManager]
    val kyuubiSession = mock[KyuubiSessionImpl]
    when(backendService.openSession(any(), any(), any(), any(), any())).thenReturn(handle)
    when(backendService.sessionManager).thenReturn(sessionMgr)
    when(sessionMgr.getSession(handle)).thenReturn(kyuubiSession)
    doNothing().when(kyuubiSession).waitForEngineLaunched()
    when(kyuubiSession.engineConnectUrl).thenReturn(Some("localhost:1"))
    manager.getOrOpen(sessionId, "john")
    handle
  }

  test("release: shut down channel and closes backend session") {
    val backendService = mock[BackendService]
    val channel = mock[ManagedChannel]
    val manager = makeManager(backendService, channel)
    val handle = openSession(manager, backendService, "s1")

    manager.release("s1")

    verify(channel).shutdownNow()
    verify(backendService).closeSession(handle)
  }

  test("release: unknown session is no-op") {
    val backendService = mock[BackendService]
    val manager = makeManager(backendService, mock[ManagedChannel])

    manager.release("nonexistent")

    verifyNoInteractions(backendService)
  }

  test("release: channel.shutdownNow throws, backend session still closed") {
    val backendService = mock[BackendService]
    val channel = mock[ManagedChannel]
    val manager = makeManager(backendService, channel)
    openSession(manager, backendService, "s1")
    doThrow(new RuntimeException("shutdown failed")).when(channel).shutdownNow()

    manager.release("s1")

    verify(backendService).closeSession(any())
  }

  test("release: backendService.closeSession throws, does not rethrow") {
    val backendService = mock[BackendService]
    val channel = mock[ManagedChannel]
    val manager = makeManager(backendService, channel)
    openSession(manager, backendService, "s1")
    doThrow(new RuntimeException("close failed")).when(backendService).closeSession(any())

    manager.release("s1")

    verify(channel).shutdownNow()
  }

  test("getOrOpen: second call with same sessionId returns cached session") {
    val backendService = mock[BackendService]
    val manager = makeManager(backendService, mock[ManagedChannel])
    openSession(manager, backendService, "s1")

    manager.getOrOpen("s1", "john")

    verify(backendService, times(1)).openSession(any(), any(), any(), any(), any())
  }

  test("getOrOpen: forwards engine profile from gRPC context into session conf") {
    val backendService = mock[BackendService]
    val manager = makeManager(backendService, mock[ManagedChannel])
    val handle = SessionHandle(java.util.UUID.randomUUID())
    val sessionMgr = mock[SessionManager]
    val kyuubiSession = mock[KyuubiSessionImpl]
    val confCaptor = ArgumentCaptor.forClass(classOf[Map[String, String]])
    when(backendService.openSession(any(), any(), any(), any(), confCaptor.capture()))
      .thenReturn(handle)
    when(backendService.sessionManager).thenReturn(sessionMgr)
    when(sessionMgr.getSession(handle)).thenReturn(kyuubiSession)
    doNothing().when(kyuubiSession).waitForEngineLaunched()
    when(kyuubiSession.engineConnectUrl).thenReturn(Some("localhost:1"))

    // Stands in for SparkConnectAuthInterceptor having read the kyuubi.engine.profile header,
    // e.g. after an HA failover reconnect landing on a different, cold KyuubiServer instance.
    val ctx = Context.current().withValue(SparkConnectAuthInterceptor.ENGINE_PROFILE_KEY, "spark42")
    val previous = ctx.attach()
    try {
      manager.getOrOpen("s1", "john")
    } finally {
      ctx.detach(previous)
    }

    assert(confCaptor.getValue.get(ENGINE_PROFILE.key) === Some("spark42"))
  }

  test("getOrOpen: no engine profile in context leaves session conf unset") {
    val backendService = mock[BackendService]
    val manager = makeManager(backendService, mock[ManagedChannel])
    val handle = SessionHandle(java.util.UUID.randomUUID())
    val sessionMgr = mock[SessionManager]
    val kyuubiSession = mock[KyuubiSessionImpl]
    val confCaptor = ArgumentCaptor.forClass(classOf[Map[String, String]])
    when(backendService.openSession(any(), any(), any(), any(), confCaptor.capture()))
      .thenReturn(handle)
    when(backendService.sessionManager).thenReturn(sessionMgr)
    when(sessionMgr.getSession(handle)).thenReturn(kyuubiSession)
    doNothing().when(kyuubiSession).waitForEngineLaunched()
    when(kyuubiSession.engineConnectUrl).thenReturn(Some("localhost:1"))

    manager.getOrOpen("s1", "john")

    assert(confCaptor.getValue.get(ENGINE_PROFILE.key) === None)
  }

  test("getOrOpen: unexpected session type closes backend session") {
    val backendService = mock[BackendService]
    val manager = makeManager(backendService, mock[ManagedChannel])
    val handle = SessionHandle(java.util.UUID.randomUUID())
    val sessionMgr = mock[SessionManager]
    when(backendService.openSession(any(), any(), any(), any(), any())).thenReturn(handle)
    when(backendService.sessionManager).thenReturn(sessionMgr)
    when(sessionMgr.getSession(handle)).thenReturn(mock[Session])

    intercept[KyuubiException] {
      manager.getOrOpen("s1", "john")
    }

    verify(backendService).closeSession(handle)
  }

  test("getOrOpen: close backend session if engine does not register a Connect URL") {
    val backendService = mock[BackendService]
    val manager = makeManager(backendService, mock[ManagedChannel])
    val handle = SessionHandle(java.util.UUID.randomUUID())
    val sessionMgr = mock[SessionManager]
    val kyuubiSession = mock[KyuubiSessionImpl]
    when(backendService.openSession(any(), any(), any(), any(), any())).thenReturn(handle)
    when(backendService.sessionManager).thenReturn(sessionMgr)
    when(sessionMgr.getSession(handle)).thenReturn(kyuubiSession)
    doNothing().when(kyuubiSession).waitForEngineLaunched()
    when(kyuubiSession.engineConnectUrl).thenReturn(None)

    intercept[org.apache.kyuubi.KyuubiException] {
      manager.getOrOpen("s1", "john")
    }

    verify(backendService).closeSession(handle)
  }

  test("closeAll: release all sessions") {
    val backendService = mock[BackendService]
    val ch1 = mock[ManagedChannel]
    val ch2 = mock[ManagedChannel]
    var callCount = 0
    val manager = new SparkConnectSessionManager(backendService) {
      override protected def buildChannel(connectUrl: String): ManagedChannel = {
        callCount += 1
        if (callCount == 1) ch1 else ch2
      }
    }
    openSession(manager, backendService, "s1")
    openSession(manager, backendService, "s2")

    manager.closeAll()

    verify(ch1).shutdownNow()
    verify(ch2).shutdownNow()
    verify(backendService, times(2)).closeSession(any())
  }
}
