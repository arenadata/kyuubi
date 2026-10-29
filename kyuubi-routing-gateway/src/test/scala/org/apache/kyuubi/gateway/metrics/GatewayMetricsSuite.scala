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

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.gateway.cluster.{ClusterRef, DeclaredCapacity, ScaleTarget}
import org.apache.kyuubi.metrics.{MetricsConf, MetricsSystem}

class GatewayMetricsSuite extends KyuubiFunSuite {

  private var metrics: MetricsSystem = _

  override def beforeEach(): Unit = {
    metrics = new MetricsSystem
    // No reporter: the registry is what is under test, and starting Jetty here
    // would make the suite fight for a port with every other run.
    metrics.initialize(KyuubiConf().set(MetricsConf.METRICS_REPORTERS.key, ""))
    metrics.start()
    // The metrics object is a singleton and outlives one test, so each test
    // starts from a known set rather than from whatever ran before it.
    GatewayMetrics.publish(Seq.empty)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    if (metrics != null) metrics.stop()
    super.afterEach()
  }

  private def gauges: Map[String, Any] =
    MetricsSystem.getMetricsRegistry.get.getGauges.asScala
      .map { case (name, gauge) => name -> gauge.getValue }
      .toMap

  private def cluster(name: String, users: Set[String] = Set("alice")) =
    ClusterRef(name, "trino", s"http://$name:8080", users = users)

  test("the discovered set is published cluster by cluster") {
    GatewayMetrics.publish(Seq(
      cluster("ns/a").copy(
        isDefault = true,
        capacity = Some(DeclaredCapacity(10L * 1024 * 1024 * 1024, 4, 10)),
        scaleTarget = Some(ScaleTarget("ns", "a", "g", "v", "clusters"))),
      cluster("ns/b")))

    val published = gauges
    assert(published(GatewayMetrics.CLUSTERS) === 2L)
    assert(published("kyuubi.gateway.cluster.ns_a.up") === 1L)
    assert(published("kyuubi.gateway.cluster.ns_a.workers") === 4L)
    assert(published("kyuubi.gateway.cluster.ns_a.max.workers") === 10L)
    assert(published("kyuubi.gateway.cluster.ns_a.default") === 1L)
    assert(published("kyuubi.gateway.cluster.ns_a.scalable") === 1L)
    assert(published("kyuubi.gateway.cluster.ns_b.default") === 0L)
    assert(published("kyuubi.gateway.cluster.ns_b.scalable") === 0L)
    assert(
      !published.contains("kyuubi.gateway.cluster.ns_b.workers"),
      "a cluster that declared no capacity must not appear to have one")
  }

  test("a cluster that disappears takes its gauges with it") {
    GatewayMetrics.publish(Seq(cluster("ns/a"), cluster("ns/b")))
    GatewayMetrics.publish(Seq(cluster("ns/a")))

    assert(gauges(GatewayMetrics.CLUSTERS) === 1L)
    assert(
      !gauges.keys.exists(_.startsWith("kyuubi.gateway.cluster.ns_b")),
      "a stale gauge would keep reporting a cluster nothing can be routed to")
  }

  test("an annotation change moves the revision even though the count does not") {
    GatewayMetrics.publish(Seq(cluster("ns/a", users = Set("alice"))))
    val before = GatewayMetrics.currentRevision

    GatewayMetrics.publish(Seq(cluster("ns/a", users = Set("alice", "bob"))))

    assert(gauges(GatewayMetrics.CLUSTERS) === 1L, "the count is unchanged, as it should be")
    assert(
      GatewayMetrics.currentRevision === before + 1,
      "an edit that leaves the count alone is exactly the change a count cannot show")
    assert(gauges("kyuubi.gateway.cluster.ns_a.users") === 2L)
  }

  test("publishing the same set again is not a change") {
    val set = Seq(cluster("ns/a"))
    GatewayMetrics.publish(set)
    val before = GatewayMetrics.currentRevision
    GatewayMetrics.publish(set)
    assert(
      GatewayMetrics.currentRevision === before,
      "the revision must mean the set changed, not that a poll happened")
  }

  test("the totals survive a rebuild of the per-cluster gauges") {
    GatewayMetrics.publish(Seq(cluster("ns/a")))
    GatewayMetrics.publish(Seq(cluster("ns/a"), cluster("ns/b")))

    // The per-cluster gauges are dropped by prefix on every change. A total
    // named under that prefix would go with them, and the endpoint would lose
    // the metric worth alerting on exactly when the set was changing.
    val published = gauges
    assert(published.contains(GatewayMetrics.CLUSTERS))
    assert(published.contains(GatewayMetrics.REVISION))
    assert(published.contains(GatewayMetrics.REFRESH_FAILURES))
    assert(published(GatewayMetrics.REVISION) === GatewayMetrics.currentRevision)
  }

  test("a failed refresh is counted, because the published set still looks healthy") {
    GatewayMetrics.publish(Seq(cluster("ns/a")))
    val before = gauges(GatewayMetrics.REFRESH_FAILURES).asInstanceOf[Long]
    GatewayMetrics.refreshFailed()
    assert(gauges(GatewayMetrics.REFRESH_FAILURES) === before + 1)
  }

  test("names that Prometheus cannot carry are made predictable, not left to the exporter") {
    assert(GatewayMetrics.sanitize("team-a/trino-prod") === "team_a_trino_prod")
    assert(GatewayMetrics.sanitize("NS/Cluster.1") === "ns_cluster_1")
  }
}
