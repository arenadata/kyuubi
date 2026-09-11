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

package org.apache.kyuubi.gateway.sizing

import org.apache.kyuubi.Logging
import org.apache.kyuubi.gateway.capacity.ClusterCapacity

/**
 * How big a cluster a query needs.
 *
 * @param memoryFactor        multiplies the planner's memory estimate
 * @param defaultWorkers      used when the planner produced no usable estimate
 * @param targetSecondsPerCpu seconds of wall time to aim for, 0 to ignore speed
 * @param coresPerWorker      cores a worker pod has, for the speed target
 */
case class SizingPolicy(
    memoryFactor: Double = 1.5,
    defaultWorkers: Int = 2,
    targetSecondsPerCpu: Double = 0d,
    coresPerWorker: Int = 1)

case class SizingDecision(
    workers: Int,
    queryMemoryBytes: Long,
    reason: String)

/**
 * Turns a planner estimate into a worker count.
 *
 * Two things this cannot do, and does not pretend to:
 *
 * Memory is a bound, not a target. Below `workersFor(memory)` the query dies on
 * the engine's per-node limit; above it, more workers buy speed rather than
 * feasibility. So the memory term sets a floor and the speed term, when
 * enabled, raises it.
 *
 * cpuCost is work, not power. Dividing it by the cluster's cores yields an
 * expected duration, so it can answer "how many workers for this long" but
 * never "how many workers are required". That is why the speed term is off by
 * default: turning it on is a policy decision about paying for latency.
 *
 * `memoryFactor` is the calibration knob and its default is a guess. The
 * planner's per-operator estimates are not the query's peak - the peak lies
 * between the largest operator and the sum of the concurrently live ones, and
 * where exactly depends on the plan shape. Comparing these estimates against
 * the actual peakUserMemoryBytes reported for finished queries is what turns
 * the guess into a number, which is why the event listener collects both.
 */
class QuerySizer(policy: SizingPolicy) extends Logging {

  def size(estimate: QueryEstimate, capacity: ClusterCapacity): SizingDecision = {
    if (!estimate.estimatesPresent) {
      // No estimate is not the same as a small query. Guessing small here would
      // let an unbounded query onto a minimal cluster and fail it on memory.
      return SizingDecision(
        workers = clamp(policy.defaultWorkers, capacity),
        queryMemoryBytes = policy.defaultWorkers.toLong * capacity.maxMemoryPerNodeBytes,
        reason = "no planner estimate, using the configured default")
    }

    val memoryBytes = (estimate.peakMemoryBytes * policy.memoryFactor).toLong
    val byMemory = math.max(1, capacity.workersFor(memoryBytes))

    val bySpeed =
      if (policy.targetSecondsPerCpu > 0 && estimate.cpuCost > 0 && policy.coresPerWorker > 0) {
        val coresNeeded = estimate.cpuCost / policy.targetSecondsPerCpu
        math.max(1, math.ceil(coresNeeded / policy.coresPerWorker).toInt)
      } else 1

    val workers = clamp(math.max(byMemory, bySpeed), capacity)
    val reason =
      if (bySpeed > byMemory) s"speed target needs $bySpeed workers, memory only $byMemory"
      else s"memory needs $byMemory workers"

    SizingDecision(workers, memoryBytes, reason)
  }

  private def clamp(workers: Int, capacity: ClusterCapacity): Int =
    math.min(math.max(workers, 1), math.max(capacity.maxWorkers, 1))
}
