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

import java.io.InputStream
import java.util.concurrent.TimeUnit

import org.sparkproject.io.grpc._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connect.client.NoServersAvailableException

class FailoverManagedChannelSuite extends KyuubiFunSuite {

  private class MockChannel extends ManagedChannel {
    @volatile var shutDownCalled = false

    override def newCall[Req, Resp](
        method: MethodDescriptor[Req, Resp],
        callOptions: CallOptions): ClientCall[Req, Resp] =
      new ClientCall[Req, Resp] {
        override def start(listener: ClientCall.Listener[Resp], headers: Metadata): Unit = {}
        override def request(numMessages: Int): Unit = {}
        override def cancel(message: String, cause: Throwable): Unit = {}
        override def halfClose(): Unit = {}
        override def sendMessage(message: Req): Unit = {}
      }

    override def authority(): String = "mock"
    override def shutdown(): ManagedChannel = { shutDownCalled = true; this }
    override def shutdownNow(): ManagedChannel = { shutDownCalled = true; this }
    override def isShutdown: Boolean = shutDownCalled
    override def isTerminated: Boolean = shutDownCalled
    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = true
  }

  private def testMethod(): MethodDescriptor[Array[Byte], Array[Byte]] = {
    val m = new MethodDescriptor.Marshaller[Array[Byte]] {
      override def stream(value: Array[Byte]): InputStream = new java.io.ByteArrayInputStream(value)
      override def parse(stream: InputStream): Array[Byte] = null
    }
    MethodDescriptor.newBuilder[Array[Byte], Array[Byte]]()
      .setType(MethodDescriptor.MethodType.UNARY)
      .setFullMethodName("test/Method")
      .setRequestMarshaller(m)
      .setResponseMarshaller(m)
      .build()
  }

  /**
   * Creates a FailoverManagedChannel whose buildChannel and resolveUrl are injected via iterators.
   * channels: first element used by init(), subsequent elements used by doFailover() calls.
   * failoverUrls: returned by resolveUrl() on each doFailover(); throws RuntimeException when empty
   */
  private def makeChannel(
      channels: Iterator[MockChannel],
      failoverUrls: Iterator[String],
      originalUrl: String = "sc://zk:2181/;serviceDiscoveryMode=zooKeeper",
      initUrl: String = "sc://host1:10199"): FailoverManagedChannel = {
    val fc = new FailoverManagedChannel(originalUrl, None) {
      override protected[kyuubi] def buildChannel(url: String): ManagedChannel = channels.next()
      override protected[kyuubi] def resolveUrl(url: String, exclude: Set[String]): String =
        if (failoverUrls.hasNext) failoverUrls.next()
        else throw new NoServersAvailableException("no servers left")
    }
    fc.init(initUrl)
    fc
  }

  test("doFailover switches inner channel and shuts down old") {
    val ch1 = new MockChannel
    val ch2 = new MockChannel
    val fc = makeChannel(Iterator(ch1, ch2), Iterator("sc://host2:10199"))

    assert(fc.innerChannel eq ch1)
    fc.doFailover("host1:10199")
    assert(fc.innerChannel eq ch2)
    assert(ch1.shutDownCalled)
  }

  test("doFailover updates currentServer") {
    val ch1 = new MockChannel
    val ch2 = new MockChannel
    val fc = makeChannel(Iterator(ch1, ch2), Iterator("sc://host2:10199"))

    assert(fc.currentServer == "host1:10199")
    fc.doFailover("host1:10199")
    assert(fc.currentServer == "host2:10199")
  }

  test("doFailover keeps dead channel when no servers available") {
    val ch1 = new MockChannel
    val fc = makeChannel(Iterator(ch1), Iterator.empty)

    fc.doFailover("host1:10199")
    assert(fc.innerChannel eq ch1)
    assert(!ch1.shutDownCalled)
  }

  test("newCall clears pendingFailedServer and triggers doFailover") {
    val ch1 = new MockChannel
    val ch2 = new MockChannel
    val fc = makeChannel(Iterator(ch1, ch2), Iterator("sc://host2:10199"))

    fc.pendingFailedServer.set("host1:10199")
    fc.newCall(testMethod(), CallOptions.DEFAULT)

    assert(fc.pendingFailedServer.get() == null)
    assert(fc.innerChannel eq ch2)
  }

  test("newCall keeps dead channel when no servers available") {
    val ch1 = new MockChannel
    val fc = makeChannel(Iterator(ch1), Iterator.empty)

    fc.pendingFailedServer.set("host1:10199")
    fc.newCall(testMethod(), CallOptions.DEFAULT)

    assert(fc.pendingFailedServer.get() == null)
    assert(fc.innerChannel eq ch1)
    assert(!ch1.shutDownCalled)
  }

  test("second concurrent UNAVAILABLE does not overwrite first failed server") {
    val ch1 = new MockChannel
    val fc = makeChannel(Iterator(ch1), Iterator.empty)

    fc.pendingFailedServer.set("host1:10199")
    val listener = new FailoverClientCallListener(
      new ClientCall.Listener[Array[Byte]] {},
      fc.pendingFailedServer,
      "host2:10199")
    listener.onClose(Status.UNAVAILABLE, new Metadata())

    assert(fc.pendingFailedServer.get() == "host1:10199")
  }
}
