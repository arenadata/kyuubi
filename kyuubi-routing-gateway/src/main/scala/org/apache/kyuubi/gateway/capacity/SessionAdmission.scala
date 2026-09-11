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

package org.apache.kyuubi.gateway.capacity

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.gateway.sizing.QueryPlanner
import org.apache.kyuubi.operation.OperationHandle

/**
 * Ties a session's operations to the reservations they hold.
 *
 * Kept apart from any one session class so both engines can use it, and so the
 * bookkeeping can be tested without constructing a session at all.
 */
class SessionAdmission(gate: AdmissionGate, cluster: ClusterRef, planner: QueryPlanner)
  extends Logging {

  private val held = new ConcurrentHashMap[OperationHandle, String]()

  /**
   * Admits a statement and runs it, releasing the reservation if starting fails.
   *
   * `run` is passed in rather than returned to, so that the reservation cannot
   * outlive a failed start: there is no window in which a caller holds a
   * reservation it has not yet bound to an operation.
   */
  def admitAndRun(statement: String)(run: AdmissionTicket => OperationHandle): OperationHandle = {
    val queryId = UUID.randomUUID().toString
    gate.admit(cluster, queryId, statement, planner) match {
      case Left(denial) => throw AdmissionRefused(cluster.name, denial)
      case Right(admitted) =>
        try {
          // The ticket reaches `run` rather than being applied here: how a
          // cluster is told to hold a query for its workers, and how it is
          // asked to carry the reservation id back, are the engine's business,
          // and this bookkeeping serves both engines.
          val handle = run(AdmissionTicket(queryId, admitted.workers))
          held.put(handle, queryId)
          handle
        } catch {
          case e: Throwable =>
            gate.release(cluster.name, queryId)
            throw e
        }
    }
  }

  /** Releases what the operation held. Safe on handles that were never admitted. */
  def finished(handle: OperationHandle): Unit =
    Option(held.remove(handle)).foreach(gate.release(cluster.name, _))

  /**
   * Releases everything still held.
   *
   * A client that disconnects mid-query would otherwise leave the cluster
   * looking permanently smaller than it is.
   */
  def releaseAll(): Unit = {
    held.values().asScala.foreach(gate.release(cluster.name, _))
    held.clear()
  }

  def outstanding: Int = held.size()
}

/**
 * What an admitted statement needs to tell the cluster.
 *
 * The reservation id travels with the query so the cluster's own view of what
 * is running can be matched back to what the gateway thinks it reserved -
 * without it, a reservation whose operation is never closed can only be
 * reclaimed by age, which is a guess.
 */
case class AdmissionTicket(reservationId: String, workers: Int)

/** Refusal carrying the reason, so the client is told what would help. */
case class AdmissionRefused(cluster: String, denial: AdmissionDenial)
  extends RuntimeException(AdmissionRefused.message(cluster, denial))

object AdmissionRefused {

  def message(cluster: String, denial: AdmissionDenial): String = denial match {
    case AdmissionDenial.Busy =>
      s"Cluster $cluster has no free capacity right now; the query was not started. " +
        "Retry when the queries in flight finish."
    case AdmissionDenial.NeedsScaleUp(workers) =>
      s"Cluster $cluster is too small for this query as it stands; it needs $workers workers. " +
        "Scale it out and retry."
    case AdmissionDenial.TooLarge(needed, max) =>
      s"This query needs ${workersText(needed)} but cluster $cluster allows at most $max. " +
        "Neither waiting nor scaling will help - reduce the query or raise the cluster ceiling."
  }

  private def workersText(n: Int): String = if (n == 1) "1 worker" else s"$n workers"
}
