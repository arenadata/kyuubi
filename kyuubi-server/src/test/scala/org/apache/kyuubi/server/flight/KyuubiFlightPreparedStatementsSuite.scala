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

package org.apache.kyuubi.server.flight

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.flight.KyuubiFlightPreparedStatements._

class KyuubiFlightPreparedStatementsSuite extends KyuubiFunSuite {

  private val now = 1000000L

  test("the result started at prepare time is used by the first execution") {
    assert(nextExecution(Cached("q"), isQuery = true, now) === UseCached("q"))
  }

  test("nothing pending: the statement runs") {
    assert(nextExecution(Idle, isQuery = true, now) === Run)
    assert(nextExecution(Idle, isQuery = false, now) === Run)
  }

  test("right after an update, its result request does not run it again") {
    assert(nextExecution(UpdateRan(now), isQuery = false, now) === EmptyResult)
    assert(nextExecution(UpdateRan(now - UpdateResultWindowMs), isQuery = false, now) ===
      EmptyResult)
  }

  test("an execution long after an update runs the statement again") {
    assert(nextExecution(UpdateRan(now - UpdateResultWindowMs - 1), isQuery = false, now) ===
      Run)
  }

  test("a query executed after going through the update path runs again") {
    assert(nextExecution(UpdateRan(now), isQuery = true, now) === Run)
  }

  test("idle prepared statements are swept every half timeout, between 1 s and 1 min") {
    assert(sweepIntervalMs(500L) === 1000L)
    assert(sweepIntervalMs(10000L) === 5000L)
    assert(sweepIntervalMs(24L * 60 * 60 * 1000) === 60000L)
  }
}
