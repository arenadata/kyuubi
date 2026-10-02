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

package org.apache.kyuubi.gateway.metrics

import java.util.Locale
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.metrics.MetricsSystem

/**
 * Publishes what the gateway discovered, so the cluster set can be watched from
 * outside the process.
 *
 * Discovery is a poll against annotations that anybody can edit, and the
 * failure that matters is silent: a Service that stopped matching the selector,
 * an annotation with a typo, a cluster that disappeared. None of that shows in
 * a log anyone is reading, and all of it changes where queries go.
 *
 * A count alone would not show it. Renaming a user in an annotation, correcting
 * a port or changing a capacity leaves the count where it was, so the set is
 * published cluster by cluster, plus a revision that moves on any change at
 * all - which is the metric to alert on, because it needs no knowledge of what
 * the set is supposed to look like.
 */
object GatewayMetrics extends Logging {

  /**
   * Totals live under `discovery`, not under `cluster`.
   *
   * The per-cluster gauges are removed as a group by prefix on every rebuild,
   * and a total whose name began with that prefix would be swept away with
   * them - leaving the one metric worth alerting on absent from the endpoint
   * exactly when the set is changing.
   */
  val CLUSTERS = "kyuubi.gateway.discovery.clusters"
  val REVISION = "kyuubi.gateway.discovery.revision"
  val REFRESH_FAILURES = "kyuubi.gateway.discovery.refresh.failures"

  /** Prefix of the per-cluster gauges, kept so they can be replaced as a group. */
  val CLUSTER_PREFIX = "kyuubi.gateway.cluster."

  private val clusterCount = new AtomicLong(0)
  private val revision = new AtomicLong(0)
  private val refreshFailures = new AtomicLong(0)
  private val published = new AtomicReference[Seq[ClusterRef]](Seq.empty)

  /** The set the per-cluster gauges currently describe, if any have been built. */
  private var gaugesFor: Option[Seq[ClusterRef]] = None

  /**
   * The registry the gauges were registered in, not a boolean.
   *
   * A flag would say "already registered" about a registry that no longer
   * exists - after a metrics system is restarted the gauges are gone and the
   * flag would stop them ever coming back, leaving a gateway that reports
   * nothing and looks fine.
   */
  private var registeredIn: Option[MetricRegistry] = None

  /**
   * Publishes the set, if it differs from what is already published.
   *
   * Idempotent on purpose. The revision is what an alert watches, so it has to
   * mean "the set changed" and not "somebody called publish" - and publish is
   * called both by the resolver on a change and once at startup, when the
   * metrics registry finally exists. A registry that appears after the first
   * call is why the gauges are rebuilt even when the set did not change.
   */
  def publish(clusters: Seq[ClusterRef]): Unit = synchronized {
    val changed = clusters != published.get()
    if (changed) {
      published.set(clusters)
      clusterCount.set(clusters.size.toLong)
      revision.incrementAndGet()
    }
    MetricsSystem.getMetricsRegistry.foreach { registry =>
      val fresh = !registeredIn.exists(_ eq registry)
      if (fresh) {
        registerTotals(registry)
        registeredIn = Some(registry)
        gaugesFor = None
      }
      if (changed || fresh || !gaugesFor.contains(clusters)) {
        rebuild(registry, clusters)
        gaugesFor = Some(clusters)
        info(s"Published ${clusters.size} clusters at revision ${revision.get()}")
      }
    }
  }

  private def rebuild(registry: MetricRegistry, clusters: Seq[ClusterRef]): Unit = {
    // Removed and rebuilt as a group: a cluster that disappeared must take its
    // gauges with it, or the metrics would keep reporting a cluster nothing can
    // be routed to.
    registry.removeMatching((name, _) => name.startsWith(CLUSTER_PREFIX))
    clusters.foreach { cluster =>
      val prefix = CLUSTER_PREFIX + sanitize(cluster.name)
      register(registry, s"$prefix.up", 1L)
      register(registry, s"$prefix.users", cluster.users.size.toLong)
      register(registry, s"$prefix.default", if (cluster.isDefault) 1L else 0L)
      register(registry, s"$prefix.scalable", if (cluster.scaleTarget.isDefined) 1L else 0L)
      cluster.capacity.foreach { capacity =>
        register(registry, s"$prefix.workers", capacity.workers.toLong)
        register(registry, s"$prefix.max.workers", capacity.maxWorkers.toLong)
        register(registry, s"$prefix.max.memory.per.node.bytes", capacity.maxMemoryPerNodeBytes)
      }
    }
  }

  /** A refresh that failed leaves the previous set published; this says it happened. */
  def refreshFailed(): Unit = refreshFailures.incrementAndGet()

  def currentRevision: Long = revision.get()

  def currentClusters: Seq[ClusterRef] = published.get()

  private def registerTotals(registry: MetricRegistry): Unit = {
    register(registry, CLUSTERS, clusterCount)
    register(registry, REVISION, revision)
    register(registry, REFRESH_FAILURES, refreshFailures)
  }

  private def register(registry: MetricRegistry, name: String, value: AtomicLong): Unit =
    registry.register(
      name,
      new Gauge[Long] {
        override def getValue: Long = value.get()
      })

  private def register(registry: MetricRegistry, name: String, value: Long): Unit =
    registry.register(
      name,
      new Gauge[Long] {
        override def getValue: Long = value
      })

  /**
   * Cluster names are `namespace/name`, and the exporter turns anything that is
   * not alphanumeric into an underscore.
   *
   * Sanitising here rather than leaving it to the exporter keeps the metric name
   * predictable from the cluster name, which is what someone writing an alert
   * needs.
   */
  private[metrics] def sanitize(cluster: String): String =
    cluster.toLowerCase(Locale.ROOT).map(c => if (c.isLetterOrDigit) c else '_')
}
