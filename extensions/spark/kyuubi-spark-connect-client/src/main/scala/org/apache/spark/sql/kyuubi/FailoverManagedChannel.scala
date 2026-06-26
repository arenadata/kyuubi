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

package org.apache.spark.sql.kyuubi

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.connect.client.SparkConnectClient
import org.sparkproject.io.grpc._

import org.apache.kyuubi.spark.connect.client.{KyuubiTokenClient, ZookeeperUrlResolver}

/**
 * A [[ManagedChannel]] wrapper that transparently fails over to the next live Kyuubi server
 * on UNAVAILABLE errors.
 *
 * When current server becomes unavailable:
 *   1. [[FailoverClientCallListener.onClose]] records failed server (non-blocking, on gRPC
 *      transport thread).
 *   2. The next [[newCall]] (issued by [[ExecutePlanResponseReattachableIterator]] for
 *      ReattachExecute) detects the pending failover, resolves the next server from ZooKeeper,
 *      and switches inner channel - all on application thread.
 *   3. [[ExecutePlanResponseReattachableIterator]] retries via ReattachExecute on the new
 *      channel, resuming the operation from last received response.
 *
 * Auth token is not refreshed after failover because tokens are stored in shared JDBC
 * token store and validated by any Kyuubi server in the cluster.
 */
private[kyuubi] class FailoverManagedChannel(
    originalUrl: String,
    tokenClient: Option[KyuubiTokenClient]) extends ManagedChannel {

  @volatile private var innerChannel: ManagedChannel = _
  @volatile private var currentServer: String = _

  // Set from gRPC transport thread in onClose; read+cleared on application thread in newCall.
  private val pendingFailedServer = new AtomicReference[String](null)

  def init(resolvedUrl: String): Unit = {
    innerChannel = buildChannel(resolvedUrl)
    currentServer = serverFrom(resolvedUrl)
  }

  private def buildChannel(resolvedUrl: String): ManagedChannel = {
    val b = SparkConnectClient.builder().connectionString(resolvedUrl)
    tokenClient.foreach(tc => b.option("authorization", s"Bearer ${tc.currentToken}"))
    SparkConnectBridge.createChannel(b.configuration)
    // b.configuration.createChannel()
  }

  private def serverFrom(resolvedUrl: String): String = {
    val builder = SparkConnectClient.builder().connectionString(resolvedUrl)
    s"${builder.host}:${builder.port}"
  }

  private def doFailover(failedServer: String): Unit = synchronized {
    val exclude = Set(failedServer)
    try {
      val newUrl = ZookeeperUrlResolver.resolve(originalUrl, exclude)
      val newChannel = buildChannel(newUrl)
      val old = innerChannel
      innerChannel = newChannel
      currentServer = serverFrom(newUrl)
      old.shutdownNow()
    } catch {
      case _: RuntimeException => // all servers exhausted; keep current (dead) channel
    }
  }

  override def newCall[Req, Resp](
      method: MethodDescriptor[Req, Resp],
      callOptions: CallOptions): ClientCall[Req, Resp] = {
    val failed = pendingFailedServer.getAndSet(null)
    if (failed != null) doFailover(failed)
    val serverAtCallTime = currentServer
    new FailoverClientCall(
      innerChannel.newCall(method, callOptions),
      pendingFailedServer,
      serverAtCallTime)
  }

  override def authority(): String = innerChannel.authority()

  override def shutdown(): ManagedChannel = { innerChannel.shutdown(); this }
  override def shutdownNow(): ManagedChannel = { innerChannel.shutdownNow(); this }
  override def isShutdown: Boolean = innerChannel.isShutdown
  override def isTerminated: Boolean = innerChannel.isTerminated
  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    innerChannel.awaitTermination(timeout, unit)
}

private[kyuubi] class FailoverClientCall[Req, Resp](
    delegate: ClientCall[Req, Resp],
    pendingFailedServer: AtomicReference[String],
    serverAtCallTime: String)
    extends ForwardingClientCall.SimpleForwardingClientCall[Req, Resp](delegate) {

  override def start(responseListener: ClientCall.Listener[Resp], headers: Metadata): Unit = {
    delegate.start(
      new FailoverClientCallListener(responseListener, pendingFailedServer, serverAtCallTime),
      headers)
  }
}

private[kyuubi] class FailoverClientCallListener[Resp](
    delegate: ClientCall.Listener[Resp],
    pendingFailedServer: AtomicReference[String],
    serverAtCallTime: String)
    extends ForwardingClientCallListener.SimpleForwardingClientCallListener[Resp](delegate) {

  override def onClose(status: Status, trailers: Metadata): Unit = {
    if (status.getCode == Status.Code.UNAVAILABLE) {
      pendingFailedServer.compareAndSet(null, serverAtCallTime)
    }
    super.onClose(status, trailers)
  }
}
