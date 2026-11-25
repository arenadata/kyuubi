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

package org.apache.kyuubi

import java.io.{ByteArrayOutputStream, IOException}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * A TCP relay on the loopback interface. It forwards every connection to the target and
 * records the bytes in both directions, so that tests can check what goes over the wire.
 * Bytes are recorded before they are forwarded: once a reply has reached the client, the
 * request that caused it has been recorded.
 */
class TcpRecordingRelay(targetHost: String, targetPort: Int) extends AutoCloseable with Logging {

  private val serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
  private val toTarget = new ByteArrayOutputStream()
  private val fromTarget = new ByteArrayOutputStream()
  private val sockets = new ConcurrentLinkedQueue[Socket]()
  private val threads = new ConcurrentLinkedQueue[Thread]()
  @volatile private var closed = false

  val port: Int = serverSocket.getLocalPort

  startThread(s"tcp-recording-relay-$port-acceptor") {
    while (!closed) {
      try {
        val client = serverSocket.accept()
        sockets.add(client)
        val target = new Socket()
        sockets.add(target)
        try {
          target.connect(new InetSocketAddress(targetHost, targetPort), 10000)
          startThread(s"tcp-recording-relay-$port-request")(pump(client, target, toTarget))
          startThread(s"tcp-recording-relay-$port-response")(pump(target, client, fromTarget))
        } catch {
          case e: IOException =>
            warn(s"Relay $port failed to connect to $targetHost:$targetPort", e)
            Try(client.close())
            Try(target.close())
        }
      } catch {
        case e: IOException if !closed => warn(s"Relay $port failed to accept", e)
        case _: IOException =>
      }
    }
  }

  /** The bytes sent by clients to the target. */
  def clientBytes: Array[Byte] = toTarget.toByteArray

  /** The bytes sent by the target to clients. */
  def targetBytes: Array[Byte] = fromTarget.toByteArray

  /** Whether the UTF-8 bytes of `text` went over the wire, in either direction. */
  def contains(text: String): Boolean = {
    val needle = new String(text.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1)
    Seq(clientBytes, targetBytes)
      .exists(bytes => new String(bytes, StandardCharsets.ISO_8859_1).contains(needle))
  }

  def reset(): Unit = {
    toTarget.reset()
    fromTarget.reset()
  }

  override def close(): Unit = {
    closed = true
    Try(serverSocket.close())
    sockets.asScala.foreach(s => Try(s.close()))
    threads.asScala.foreach(_.join(10000))
  }

  private def pump(from: Socket, to: Socket, record: ByteArrayOutputStream): Unit = {
    val buffer = new Array[Byte](8192)
    try {
      val in = from.getInputStream
      val out = to.getOutputStream
      var n = in.read(buffer)
      while (n >= 0) {
        record.write(buffer, 0, n)
        out.write(buffer, 0, n)
        out.flush()
        n = in.read(buffer)
      }
      // pass the half close on, the other direction may still be busy
      Try(to.shutdownOutput())
    } catch {
      case _: IOException =>
        Try(from.close())
        Try(to.close())
    }
  }

  private def startThread(name: String)(body: => Unit): Thread = {
    val thread = new Thread(
      new Runnable {
        override def run(): Unit = body
      },
      name)
    thread.setDaemon(true)
    threads.add(thread)
    thread.start()
    thread
  }
}
