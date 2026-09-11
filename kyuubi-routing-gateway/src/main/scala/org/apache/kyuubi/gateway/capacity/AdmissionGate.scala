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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.gateway.scaling.ClusterScaler
import org.apache.kyuubi.gateway.sizing.{ExplainParser, QueryEstimate, QueryPlanner, QuerySizer}

/**
 * Sizes a query, then admits it if the cluster can hold it.
 *
 * The two steps belong together because the second needs the first: admission
 * subtracts a size from the remaining capacity, and the only source of that
 * size before execution is the planner.
 *
 * `capacityOf` is injected rather than reached for, so the decision logic can
 * be exercised without a cluster behind it. The planner arrives per call rather
 * than per gate because it belongs to the session: the estimate depends on the
 * catalog, schema and statistics that session sees.
 */
class AdmissionGate(
    accountant: CapacityAccountant,
    sizer: QuerySizer,
    capacityOf: ClusterRef => Option[ClusterCapacity],
    scaler: Option[ClusterScaler] = None)
  extends Logging {

  import AdmissionGate._

  case class Admitted(reservation: Option[Reservation], workers: Int)

  def admit(
      cluster: ClusterRef,
      queryId: String,
      statement: String,
      planner: QueryPlanner): Either[AdmissionDenial, Admitted] = {
    val capacity = capacityOf(cluster).getOrElse {
      // A cluster whose capacity nobody declared cannot be accounted for.
      // Refusing its queries would break every cluster not yet annotated, so
      // it is admitted unaccounted - but noisily, because an unaccounted
      // cluster silently defeats the point of having a gate.
      warnUnaccounted(cluster.name)
      return Right(Admitted(None, 0))
    }

    val estimate =
      if (!needsSizing(statement)) {
        // Metadata and session statements do not touch the data path. Planning
        // them costs a round trip and tells us nothing.
        QueryEstimate.unknown
      } else {
        try {
          ExplainParser.parse(planner.explain(statement))
        } catch {
          case e: Exception =>
            // A failed EXPLAIN is not a reason to refuse the query - the
            // statement may be one the planner cannot plan in isolation. Fall
            // back to the default size and let the engine judge it.
            warn(s"EXPLAIN failed for $queryId, sizing with the default: ${e.getMessage}")
            QueryEstimate.unknown
        }
      }

    val decision = sizer.size(estimate, capacity)
    accountant.admit(cluster.name, capacity, queryId, decision.queryMemoryBytes) match {
      case Right(reservation) =>
        info(s"Admitted $queryId to ${cluster.name}: ${decision.reason}")
        Right(Admitted(Some(reservation), decision.workers))
      case Left(AdmissionDenial.NeedsScaleUp(needed)) =>
        growAndAdmit(cluster, capacity, queryId, decision.queryMemoryBytes, needed)
      case Left(denial) =>
        info(s"Denied $queryId on ${cluster.name}: $denial")
        Left(denial)
    }
  }

  /**
   * Grows the cluster for a query that does not fit it as it stands.
   *
   * The query is then admitted against the size that was asked for, not the
   * size that exists - the workers are not there yet. That is safe only because
   * the caller sends the worker count with the query: Trino holds it in
   * WAITING_FOR_RESOURCES until they register, so a reservation made ahead of
   * the workers cannot turn into a query running without them. A scale-up that
   * never lands shows as a query that waited and failed, which is visible,
   * rather than as one that quietly ran on too few workers.
   */
  private def growAndAdmit(
      cluster: ClusterRef,
      capacity: ClusterCapacity,
      queryId: String,
      memoryBytes: Long,
      needed: Int): Either[AdmissionDenial, Admitted] = {
    val target = for {
      s <- scaler
      t <- cluster.scaleTarget
    } yield (s, t)

    target match {
      case None =>
        info(s"Denied $queryId on ${cluster.name}: needs $needed workers and nothing can scale it")
        Left(AdmissionDenial.NeedsScaleUp(needed))
      case Some((clusterScaler, scaleTarget)) =>
        val grown = clusterScaler.ensureAtLeast(scaleTarget, math.min(needed, capacity.maxWorkers))
        if (grown < needed) {
          info(s"Denied $queryId on ${cluster.name}: asked for $needed workers, got $grown")
          Left(AdmissionDenial.NeedsScaleUp(needed))
        } else {
          accountant.admit(cluster.name, capacity.copy(workers = grown), queryId, memoryBytes)
            .map { reservation =>
              info(s"Admitted $queryId to ${cluster.name} after scaling to $grown workers")
              Admitted(Some(reservation), grown)
            }
        }
    }
  }

  def release(cluster: String, queryId: String): Unit = accountant.release(cluster, queryId)

  private val warnedUnaccounted = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()

  /** Warns once per cluster; per query this would drown the log. */
  private def warnUnaccounted(cluster: String): Unit =
    if (warnedUnaccounted.add(cluster)) {
      warn(s"Cluster $cluster declares no capacity, admitting its queries unaccounted. " +
        "Annotate it with max-memory-per-node-bytes, workers and max-workers to gate it.")
    }
}

object AdmissionGate {

  /**
   * Whether a statement is worth planning before admission.
   *
   * Everything that reads data is; metadata lookups, session settings and
   * EXPLAIN itself are not. The list is deliberately a denial list rather than
   * an allow list: an unrecognised statement gets planned, which is the safe
   * direction - the cost is one round trip, whereas skipping the estimate on a
   * real query would admit it as if it were free.
   */
  def needsSizing(statement: String): Boolean = {
    val head = statement.trim.takeWhile(!_.isWhitespace).toUpperCase
    !skipped.contains(head)
  }

  private val skipped = Set(
    "EXPLAIN",
    "SET",
    "RESET",
    "USE",
    "SHOW",
    "DESCRIBE",
    "DESC",
    "PREPARE",
    "DEALLOCATE",
    "START",
    "COMMIT",
    "ROLLBACK")
}
