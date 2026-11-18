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

/**
 * What the next execution of a Flight SQL prepared statement gets.
 *
 * Queries start when they are prepared (the result schema is known only once a statement
 * runs); the first execution gets that result. The Arrow Flight SQL JDBC driver (DBeaver)
 * runs an update with DoPut and then also asks for its result with GetFlightInfo; that
 * request must not run the statement again (DDL would fail, DML would be applied twice).
 * After a DoPut, every result request (also client retries, however late) gets an empty
 * result until the next DoPut runs the update again or the handle is closed (by the client
 * or the idle sweeper). Clients re-execute updates with a new DoPut; a statement that was
 * never sent through DoPut still runs on every GetFlightInfo.
 */
private[flight] object KyuubiFlightPreparedStatements {

  /** Pending work of a prepared statement, see [[nextExecution]]. */
  sealed trait Pending[+Q]

  /** The result started at prepare time. */
  final case class Cached[Q](result: Q) extends Pending[Q]

  /** The statement ran through the update path (DoPut). */
  case object UpdateRan extends Pending[Nothing]

  case object Idle extends Pending[Nothing]

  /** What GetFlightInfo of a prepared statement does. */
  sealed trait Execution[+Q]

  final case class UseCached[Q](result: Q) extends Execution[Q]

  /** Empty result, the statement is not run again. */
  case object EmptyResult extends Execution[Nothing]

  case object Run extends Execution[Nothing]

  /**
   * How often idle prepared statements are swept: half the idle timeout, between 1 second
   * and 1 minute, so a statement is released at most about a minute after it expires.
   */
  def sweepIntervalMs(idleTimeoutMs: Long): Long =
    math.min(math.max(idleTimeoutMs / 2, 1000L), 60000L)

  /**
   * What a GetFlightInfo execution does, and the pending work it leaves. The prepare-time
   * result is used once; the marker of an update that ran stays, so no result request runs
   * the update again (a query sent through the update path runs normally).
   */
  def nextExecution[Q](pending: Pending[Q], isQuery: Boolean): (Execution[Q], Pending[Q]) =
    pending match {
      case Cached(result) => (UseCached(result), Idle)
      case UpdateRan if !isQuery => (EmptyResult, UpdateRan)
      case _ => (Run, Idle)
    }
}
