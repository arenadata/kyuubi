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

import java.util.concurrent.{Callable, ExecutionException, Executors, TimeoutException, TimeUnit}

import scala.concurrent.duration.FiniteDuration

import org.scalatest.Assertions.fail

import org.apache.kyuubi.util.NamedThreadFactory

/**
 * Runs a call that may block in a socket read with a deadline. scalatest's failAfter cannot
 * interrupt such a call, so a regression that removes a timeout would hang the whole run
 * instead of failing one test.
 */
object BoundedCall {

  /** The result of `f`, or the exception it threw, unless it is still running at `timeout`. */
  def within[T](timeout: FiniteDuration)(f: => T): T = {
    val executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("bounded-call", true))
    try {
      val result = executor.submit(new Callable[T] { override def call(): T = f })
      try result.get(timeout.toMillis, TimeUnit.MILLISECONDS)
      catch {
        case e: ExecutionException => throw e.getCause
        case _: TimeoutException => fail(s"The call did not finish within $timeout")
      }
    } finally {
      // the blocked thread is a daemon, it cannot keep the test JVM alive
      executor.shutdownNow()
    }
  }
}
