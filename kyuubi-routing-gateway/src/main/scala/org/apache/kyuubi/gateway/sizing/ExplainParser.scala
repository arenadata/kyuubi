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

import scala.collection.JavaConverters._

import com.fasterxml.jackson.core.JsonParser.Feature
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.kyuubi.Logging

/**
 * Reads the estimates out of `EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)`.
 *
 * The parser walks the whole document looking for `estimates` arrays rather
 * than following a known path. The top-level shape differs between plan types -
 * a fragment map for DISTRIBUTED, a bare node for LOGICAL - and it is not a
 * stable contract across Trino versions, while the estimate objects themselves
 * have been stable. Walking costs nothing on a plan document and removes a
 * whole class of version-upgrade breakage.
 *
 * Estimate fields come from Trino's PlanNodeStatsAndCostSummary: outputRowCount,
 * outputSizeInBytes, cpuCost, memoryCost, networkCost.
 */
object ExplainParser extends Logging {

  // Jackson writes an unestimatable value as the quoted string "NaN" (verified
  // against the version in use), which `finite` rejects field by field. Some
  // producers emit a bare NaN token instead, which is not valid JSON and would
  // otherwise cost the whole document rather than the one field.
  private val mapper = new ObjectMapper().configure(Feature.ALLOW_NON_NUMERIC_NUMBERS, true)

  def parse(json: String): QueryEstimate = {
    try {
      collect(mapper.readTree(json))
    } catch {
      case e: Exception =>
        warn(s"Could not parse the EXPLAIN output, treating the size as unknown: ${e.getMessage}")
        QueryEstimate.unknown
    }
  }

  private def collect(root: JsonNode): QueryEstimate = {
    var peakMemory = 0d
    var totalMemory = 0d
    var cpu = 0d
    var rows = 0d
    var seen = false

    def visit(node: JsonNode): Unit = {
      if (node.isObject) {
        val estimates = node.get("estimates")
        if (estimates != null && estimates.isArray) {
          estimates.elements().asScala.foreach { est =>
            val memory = finite(est, "memoryCost")
            val cpuCost = finite(est, "cpuCost")
            val rowCount = finite(est, "outputRowCount")
            if (memory.isDefined || cpuCost.isDefined || rowCount.isDefined) seen = true
            memory.foreach { m =>
              peakMemory = math.max(peakMemory, m)
              totalMemory += m
            }
            cpuCost.foreach(cpu += _)
            rowCount.foreach(r => rows = math.max(rows, r))
          }
        }
        node.elements().asScala.foreach(visit)
      } else if (node.isArray) {
        node.elements().asScala.foreach(visit)
      }
    }

    visit(root)
    if (!seen) {
      QueryEstimate.unknown
    } else {
      QueryEstimate(
        peakMemoryBytes = peakMemory.toLong,
        totalMemoryBytes = totalMemory.toLong,
        cpuCost = cpu,
        outputRowCount = rows,
        estimatesPresent = true)
    }
  }

  /**
   * Reads a numeric field, rejecting NaN and infinity.
   *
   * Trino writes NaN when it cannot estimate - typically missing table
   * statistics. Letting one through would poison every arithmetic downstream
   * and, worse, would look like a real number to the sizing formula.
   */
  private def finite(node: JsonNode, field: String): Option[Double] = {
    val value = node.get(field)
    if (value == null || !value.isNumber) return None
    val d = value.asDouble()
    if (d.isNaN || d.isInfinite || d < 0) None else Some(d)
  }
}
