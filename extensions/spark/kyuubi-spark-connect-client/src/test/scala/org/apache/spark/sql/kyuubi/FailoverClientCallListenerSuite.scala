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

// In org.apache.spark.sql.kyuubi to access private[kyuubi] FailoverClientCallListener.
package org.apache.spark.sql.kyuubi

import java.util.concurrent.atomic.AtomicReference

import org.apache.kyuubi.KyuubiFunSuite
import org.sparkproject.io.grpc.{ClientCall, Metadata, Status}

class FailoverClientCallListenerSuite extends KyuubiFunSuite {

  private def noopListener[T]: ClientCall.Listener[T] = new ClientCall.Listener[T] {}

  test("onClose UNAVAILABLE sets pendingFailedServer") {
    val pending = new AtomicReference[String](null)
    val listener =
      new FailoverClientCallListener(noopListener[String], pending, "host1:10199")
    listener.onClose(Status.UNAVAILABLE, new Metadata())
    assert(pending.get() == "host1:10199")
  }

  test("onClose OK does not set pendingFailedServer") {
    val pending = new AtomicReference[String](null)
    val listener =
      new FailoverClientCallListener(noopListener[String], pending, "host1:10199")
    listener.onClose(Status.OK, new Metadata())
    assert(pending.get() == null)
  }

  test("onClose CANCELLED does not set pendingFailedServer") {
    val pending = new AtomicReference[String](null)
    val listener =
      new FailoverClientCallListener(noopListener[String], pending, "host1:10199")
    listener.onClose(Status.CANCELLED, new Metadata())
    assert(pending.get() == null)
  }

  test("onClose INTERNAL does not set pendingFailedServer") {
    val pending = new AtomicReference[String](null)
    val listener =
      new FailoverClientCallListener(noopListener[String], pending, "host1:10199")
    listener.onClose(Status.INTERNAL, new Metadata())
    assert(pending.get() == null)
  }

  test("second UNAVAILABLE from different stream does not overwrite first failed server") {
    val pending = new AtomicReference[String](null)
    val listener1 =
      new FailoverClientCallListener(noopListener[String], pending, "host1:10199")
    val listener2 =
      new FailoverClientCallListener(noopListener[String], pending, "host2:10199")
    listener1.onClose(Status.UNAVAILABLE, new Metadata())
    listener2.onClose(Status.UNAVAILABLE, new Metadata())
    assert(pending.get() == "host1:10199")
  }

  test("onClose delegates to wrapped listener") {
    var delegateCalled = false
    val delegate = new ClientCall.Listener[String] {
      override def onClose(status: Status, trailers: Metadata): Unit =
        delegateCalled = true
    }
    val pending = new AtomicReference[String](null)
    val listener = new FailoverClientCallListener(delegate, pending, "host1:10199")
    listener.onClose(Status.OK, new Metadata())
    assert(delegateCalled)
  }
}
