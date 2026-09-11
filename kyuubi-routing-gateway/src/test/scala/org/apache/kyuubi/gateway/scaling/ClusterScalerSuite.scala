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

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite

class ClusterScalerSuite extends KyuubiFunSuite {

  private val path = Seq("spec", "worker", "replicas")

  test("the patch touches only the replica count") {
    val patch = KubernetesClusterScaler.nest(path, 6).asScala
    // Anything broader would replace the operator's own fields on a merge.
    assert(patch.keySet === Set("spec"))
    val spec = patch("spec").asInstanceOf[Map[String, Any]]
    assert(spec.keySet === Set("worker"))
    assert(spec("worker").asInstanceOf[Map[String, Any]] === Map("replicas" -> 6))
  }

  test("an empty path is refused rather than patching the whole object") {
    intercept[IllegalArgumentException](KubernetesClusterScaler.nest(Seq.empty, 1))
  }

  test("reads the replica count out of a parsed resource") {
    val resource = Map[String, Any](
      "spec" -> Map[String, Any](
        "coordinator" -> Map[String, Any]("replicas" -> 1),
        "worker" -> Map[String, Any]("replicas" -> 4)).asJava).asJava.asScala.toMap
    assert(KubernetesClusterScaler.dig(resource, path) === Some(4))
    assert(KubernetesClusterScaler.dig(
      resource,
      Seq("spec", "coordinator", "replicas")) === Some(1))
  }

  test("a spec that never had a replica count reads as absent, not as zero") {
    val resource = Map[String, Any]("spec" -> Map[String, Any]("worker" -> Map.empty).asJava)
    assert(
      KubernetesClusterScaler.dig(resource, path).isEmpty,
      "absent must not be mistaken for zero - zero would look like a scaled-in cluster")
    assert(KubernetesClusterScaler.dig(Map.empty, path).isEmpty)
    assert(KubernetesClusterScaler.dig(
      resource,
      Seq("spec", "worker", "replicas", "deeper")).isEmpty)
  }
}
