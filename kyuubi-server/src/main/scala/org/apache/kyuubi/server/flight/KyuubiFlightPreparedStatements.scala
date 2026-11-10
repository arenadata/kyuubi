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
 * request must not run the statement again (DDL would fail, DML would be applied twice),
 * so it gets an empty result when it comes within [[UpdateResultWindowMs]] of the update.
 */
private[flight] object KyuubiFlightPreparedStatements {

  /** Pending work of a prepared statement; taken (and reset to [[Idle]]) by an execution. */
  sealed trait Pending[+Q]

  /** The result started at prepare time. */
  final case class Cached[Q](result: Q) extends Pending[Q]

  /** The statement ran through the update path (DoPut) at `atMs`. */
  final case class UpdateRan(atMs: Long) extends Pending[Nothing]

  case object Idle extends Pending[Nothing]

  /** What GetFlightInfo of a prepared statement does. */
  sealed trait Execution[+Q]

  final case class UseCached[Q](result: Q) extends Execution[Q]

  /** Empty result, the statement is not run again. */
  case object EmptyResult extends Execution[Nothing]

  case object Run extends Execution[Nothing]

  /** The JDBC driver asks for the result right after the update. */
  val UpdateResultWindowMs: Long = 60000L

  def nextExecution[Q](pending: Pending[Q], isQuery: Boolean, nowMs: Long): Execution[Q] =
    pending match {
      case Cached(result) => UseCached(result)
      case UpdateRan(atMs) if !isQuery && nowMs - atMs <= UpdateResultWindowMs => EmptyResult
      case _ => Run
    }
}
