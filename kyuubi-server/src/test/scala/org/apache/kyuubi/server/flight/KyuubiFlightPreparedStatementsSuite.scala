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

  /** Executions of `n` GetFlightInfo requests in a row, starting from `pending`. */
  private def executions[Q](pending: Pending[Q], isQuery: Boolean, n: Int): Seq[Execution[Q]] =
    Iterator.iterate[(Option[Execution[Q]], Pending[Q])]((None, pending)) { case (_, p) =>
      val (execution, next) = nextExecution(p, isQuery)
      (Some(execution), next)
    }.drop(1).take(n).flatMap(_._1).toSeq

  test("the result started at prepare time is used by the first execution only") {
    assert(nextExecution(Cached("q"), isQuery = true) === (UseCached("q"), Idle))
    assert(executions(Cached("q"), isQuery = true, 3) === Seq(UseCached("q"), Run, Run))
  }

  test("nothing pending: the statement runs on every execution") {
    assert(executions(Idle, isQuery = true, 3) === Seq(Run, Run, Run))
    // a prepared update executed only through GetFlightInfo (never DoPut) runs every time
    assert(executions(Idle, isQuery = false, 3) === Seq(Run, Run, Run))
  }

  test("after an update ran, no result request runs it again, and the marker stays") {
    assert(nextExecution(UpdateRan, isQuery = false) === (EmptyResult, UpdateRan))
    // retries, however late: there is no time window
    assert(executions(UpdateRan, isQuery = false, 100).forall(_ == EmptyResult))
  }

  test("a query executed after going through the update path runs again") {
    assert(nextExecution(UpdateRan, isQuery = true) === (Run, Idle))
  }

  test("idle prepared statements are swept every half timeout, between 1 s and 1 min") {
    assert(sweepIntervalMs(500L) === 1000L)
    assert(sweepIntervalMs(10000L) === 5000L)
    assert(sweepIntervalMs(24L * 60 * 60 * 1000) === 60000L)
  }
}
