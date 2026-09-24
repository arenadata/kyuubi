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

import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext

import org.apache.kyuubi.Logging
import org.apache.kyuubi.gateway.cluster.ScaleTarget

/**
 * The `scale` subresource of a cluster's custom resource.
 *
 * Kept behind an interface because it is the one part that needs a live api
 * server, and the decision about when to scale is worth testing on its own.
 */
trait ScaleApi {

  /**
   * Workers the cluster actually has, as the resource reports them.
   *
   * `status.replicas`, not `spec.replicas`: the question admission is asking is
   * how many workers exist, and the spec answers how many were asked for. A
   * cluster still bringing up the six it was told to run has fewer than six to
   * put a query on.
   */
  def ready(target: ScaleTarget): Option[Int]

  /** Asks for `workers`, without waiting for them. */
  def request(target: ScaleTarget, workers: Int): Unit
}

/**
 * Raises a cluster's worker count through its `scale` subresource.
 *
 * `autoscaling/v1.Scale` rather than a patch of the spec: the subresource is
 * the declared way to resize a custom resource, it keeps the gateway from
 * knowing where in the spec the count lives, and it narrows the permission the
 * gateway needs from writing `clusters` to writing `clusters/scale` - so a
 * gateway that can resize a cluster still cannot change its image or its
 * catalogs.
 */
class KubernetesClusterScaler(api: ScaleApi) extends ClusterScaler with Logging {

  // One monitor per target, not one for the whole scaler: admissions on
  // different clusters have no reason to contend. Within one target, this
  // closes the read-then-write race between two concurrent scale-ups in this
  // process - without it, a smaller concurrent request can issue its PUT
  // after a larger one and leave the cluster short of what the larger
  // admission already believes it was granted. It cannot close that race
  // across gateway replicas, which still need a lock or a CAS-capable scale
  // API to be fully safe.
  private val locks = new ConcurrentHashMap[ScaleTarget, Object]()

  override def ensureAtLeast(target: ScaleTarget, workers: Int): Int = {
    val lock = locks.computeIfAbsent(target, _ => new Object)
    lock.synchronized {
      try {
        api.ready(target) match {
          case Some(current) if current >= workers =>
            debug(s"$target already has $current workers, no scale-up needed for $workers")
            current
          case current =>
            api.request(target, workers)
            info(s"Asked $target for $workers workers, had ${current.getOrElse(0)}")
            workers
        }
      } catch {
        case NonFatal(e) =>
          // Report nothing rather than what was asked for: the caller sizes an
          // admission against the answer, and claiming workers that were never
          // requested would admit a query onto a cluster that cannot hold it.
          warn(s"Could not scale $target to $workers workers", e)
          0
      }
    }
  }
}

/** Talks to the api server. */
class FabricScaleApi(client: KubernetesClient) extends ScaleApi {

  override def ready(target: ScaleTarget): Option[Int] =
    Option(resource(target).scale())
      .flatMap(s => Option(s.getStatus))
      .flatMap(s => Option(s.getReplicas))
      .map(_.intValue())

  override def request(target: ScaleTarget, workers: Int): Unit =
    resource(target).scale(workers)

  private def resource(target: ScaleTarget) = {
    val context = new ResourceDefinitionContext.Builder()
      .withGroup(target.group)
      .withVersion(target.version)
      .withPlural(target.plural)
      .withNamespaced(true)
      .build()
    client.genericKubernetesResources(context)
      .inNamespace(target.namespace)
      .withName(target.name)
  }
}
