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

package org.apache.kyuubi.gateway.scaling

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.gateway.cluster.ScaleTarget

class ClusterScalerSuite extends KyuubiFunSuite {

  private val target = ScaleTarget("ns", "trino-a", "trino.arenadata.io", "v1alpha1", "clusters")

  private class FakeScale(var readyNow: Option[Int], fail: Boolean = false) extends ScaleApi {
    var requested: Option[Int] = None
    override def ready(t: ScaleTarget): Option[Int] =
      if (fail) throw new RuntimeException("api server said no") else readyNow
    override def request(t: ScaleTarget, workers: Int): Unit = {
      if (fail) throw new RuntimeException("api server said no")
      requested = Some(workers)
    }
  }

  test("a cluster that already has the workers is not touched") {
    val api = new FakeScale(Some(6))
    assert(new KubernetesClusterScaler(api).ensureAtLeast(target, 4) === 6)
    assert(api.requested.isEmpty, "resizing a cluster that already fits churns pods for nothing")
  }

  test("a cluster with too few workers is asked for more") {
    val api = new FakeScale(Some(2))
    assert(new KubernetesClusterScaler(api).ensureAtLeast(target, 6) === 6)
    assert(api.requested === Some(6))
  }

  test("readiness is what counts, not what was asked for earlier") {
    // The spec may already say six while only two are up. Trusting the spec
    // would skip the request and admit a query onto workers that are not there.
    val api = new FakeScale(Some(2))
    new KubernetesClusterScaler(api).ensureAtLeast(target, 6)
    assert(api.requested === Some(6))
  }

  test("a resource that reports no replicas yet is scaled, not assumed full") {
    val api = new FakeScale(None)
    assert(new KubernetesClusterScaler(api).ensureAtLeast(target, 3) === 3)
    assert(api.requested === Some(3))
  }

  test("a failure reports no workers rather than the ones it asked for") {
    val api = new FakeScale(Some(1), fail = true)
    assert(
      new KubernetesClusterScaler(api).ensureAtLeast(target, 6) === 0,
      "claiming workers that were never requested admits a query onto a cluster without them")
  }
}
