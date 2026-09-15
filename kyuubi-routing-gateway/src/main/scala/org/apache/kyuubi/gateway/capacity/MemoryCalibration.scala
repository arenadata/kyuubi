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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

import scala.collection.JavaConverters._

import org.apache.kyuubi.Logging

/**
 * Compares what the gateway reserved against what the query actually used.
 *
 * `memoryFactor` is the one number in sizing that cannot be derived - it stands
 * for how far the planner's estimate is from reality on a particular workload,
 * and that is measured, not reasoned about. This accumulates the measurement so
 * the factor can be set from evidence.
 *
 * It reports and does not act. A factor that moved on its own would change how
 * queries are admitted without anyone deciding to, and the first sign of a bad
 * measurement would be queries being refused.
 */
class MemoryCalibration extends Logging {

  private case class Tally(samples: LongAdder, reserved: LongAdder, actual: LongAdder)

  private val tallies = new ConcurrentHashMap[String, Tally]()

  def record(cluster: String, reservation: Reservation, peakMemoryBytes: Option[Long]): Unit =
    peakMemoryBytes.filter(_ > 0).foreach { actual =>
      val tally = tallies.computeIfAbsent(
        cluster,
        _ => Tally(new LongAdder, new LongAdder, new LongAdder))
      tally.samples.increment()
      tally.reserved.add(reservation.memoryBytes)
      tally.actual.add(actual)
    }

  /**
   * The factor that would have reserved exactly what was used, per cluster.
   *
   * Reserved over actual, aggregated rather than averaged per query: what
   * matters for capacity is the total held versus the total needed, and a
   * per-query average would let a crowd of tiny queries outvote the large ones
   * that actually decide whether a cluster fits its work.
   */
  def observedFactor(cluster: String): Option[Double] =
    Option(tallies.get(cluster))
      .filter(t => t.samples.sum() > 0 && t.actual.sum() > 0)
      .map(t => t.reserved.sum().toDouble / t.actual.sum().toDouble)

  def samples(cluster: String): Long =
    Option(tallies.get(cluster)).map(_.samples.sum()).getOrElse(0L)

  /** Logs what has been seen, for an operator deciding what to set. */
  def report(): Unit = tallies.asScala.foreach { case (cluster, tally) =>
    observedFactor(cluster).foreach { factor =>
      info(f"Calibration for $cluster over ${tally.samples.sum()} queries: " +
        f"reserved/used = $factor%.2f of the configured memoryFactor")
    }
  }
}
