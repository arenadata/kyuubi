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

/**
 * How a cluster is shared between concurrent queries.
 *
 * Packing is safe for memory - the engine enforces its own per-node limit - but
 * not for time: co-resident queries run on the same workers and share CPU, so
 * both slow down. Which of the two matters is a property of the workload, not
 * something that can be decided centrally, hence the choice.
 */
object AdmissionPolicy extends Enumeration {
  type AdmissionPolicy = Value

  /** Admit while memory is left. Highest utilisation, query time varies. */
  val PackByMemory,

  /** One query at a time. Predictable time, the cluster idles between queries. */
  Exclusive = Value
}

/**
 * What a cluster can offer, as known outside the engine.
 *
 * @param maxMemoryPerNodeBytes the engine's own per-node query memory limit
 * @param workers               workers currently able to accept work
 * @param maxWorkers            ceiling the cluster may be scaled to
 */
case class ClusterCapacity(
    maxMemoryPerNodeBytes: Long,
    workers: Int,
    maxWorkers: Int) {

  /** Memory available across the workers that exist right now. */
  def totalMemoryBytes: Long = maxMemoryPerNodeBytes * workers

  /** Memory available if the cluster were scaled to its ceiling. */
  def maxMemoryBytes: Long = maxMemoryPerNodeBytes * maxWorkers

  /**
   * Workers needed to hold a query of this size.
   *
   * This is a lower bound, not a target: below it the query fails on the
   * engine's per-node memory limit. How many workers it should get for speed is
   * a separate question, and one the estimate cannot answer - cpuCost is work,
   * not power.
   */
  def workersFor(queryMemoryBytes: Long): Int =
    if (maxMemoryPerNodeBytes <= 0) 0
    else math.ceil(queryMemoryBytes.toDouble / maxMemoryPerNodeBytes).toInt
}

/**
 * A query admitted to a cluster and not yet finished.
 *
 * @param synthetic holds capacity for something that is not a Trino query the
 *                  coordinator will ever list - a shrink-in-progress worker,
 *                  for instance. [[ReservationReconciler]] cannot judge these
 *                  by the cluster's own query list the way it judges real
 *                  reservations, so it leaves them alone; whoever admitted one
 *                  is responsible for releasing it.
 */
case class Reservation(
    queryId: String,
    memoryBytes: Long,
    admittedAtMillis: Long,
    synthetic: Boolean = false)

/** Why a query could not be admitted right now. */
sealed trait AdmissionDenial
object AdmissionDenial {

  /** Capacity exists but is taken; scaling out would not help, waiting will. */
  case object Busy extends AdmissionDenial

  /** The cluster is too small as it stands, but scaling to `workers` would fit. */
  case class NeedsScaleUp(workers: Int) extends AdmissionDenial

  /** Beyond the cluster's ceiling - waiting and scaling are both futile. */
  case class TooLarge(neededWorkers: Int, maxWorkers: Int) extends AdmissionDenial
}
