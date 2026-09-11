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

/**
 * What the planner thinks a query will cost, as read from EXPLAIN.
 *
 * @param peakMemoryBytes largest single-operator memory estimate in the plan
 * @param totalMemoryBytes sum over all operators
 * @param cpuCost          processor work, not power - see [[QuerySizer]]
 * @param outputRowCount   rows the query is expected to return
 * @param estimatesPresent whether the planner produced any estimate at all
 */
case class QueryEstimate(
    peakMemoryBytes: Long,
    totalMemoryBytes: Long,
    cpuCost: Double,
    outputRowCount: Double,
    estimatesPresent: Boolean)

object QueryEstimate {

  /**
   * What a plan with no usable estimates yields.
   *
   * Trino emits NaN estimates when table statistics are missing, which is the
   * normal case for freshly written data. Callers must treat this as "size not
   * known" and fall back to a default, never as "this query is free".
   */
  val unknown: QueryEstimate = QueryEstimate(0L, 0L, 0d, 0d, estimatesPresent = false)
}
