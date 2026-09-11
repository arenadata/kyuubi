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

import scala.util.control.NonFatal

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.kyuubi.Logging

/**
 * The worker that a scale-down would remove.
 *
 * @param url      where to reach it, built from its stable DNS name
 * @param replicas how many workers the pool has now
 */
case class DepartingWorker(url: String, replicas: Int)

/**
 * Which worker Kubernetes will remove when the replica count drops.
 *
 * Only a StatefulSet can answer this: it removes the highest ordinal, and the
 * pod keeps a stable name and DNS record, so the worker to drain can be named
 * before anything is deleted.
 *
 * A Deployment cannot. Its ReplicaSet controller picks the victim, and
 * `controller.kubernetes.io/pod-deletion-cost` only biases that choice - the
 * specification calls it best-effort. Draining one worker and having Kubernetes
 * remove another means killing the queries on the other, which is precisely the
 * failure shrinking exists to avoid, so a pool that is not a StatefulSet is
 * refused rather than shrunk on a guess.
 */
trait WorkerPool {

  /**
   * The worker that would go, or None when this pool cannot be shrunk safely.
   */
  def departing(namespace: String, statefulSet: String, scheme: String, port: Int)
      : Option[DepartingWorker]
}

class KubernetesWorkerPool(client: KubernetesClient) extends WorkerPool with Logging {

  override def departing(
      namespace: String,
      statefulSet: String,
      scheme: String,
      port: Int): Option[DepartingWorker] = {
    try {
      val sts = client.apps().statefulSets().inNamespace(namespace).withName(statefulSet).get()
      if (sts == null) {
        warn(s"No StatefulSet $namespace/$statefulSet - a Deployment-backed pool cannot be " +
          "shrunk safely, because Kubernetes chooses which pod goes")
        return None
      }
      val replicas = Option(sts.getSpec.getReplicas).map(_.intValue()).getOrElse(0)
      if (replicas <= 0) return None

      // The governing Service is read rather than assumed: the pod's DNS name is
      // <sts>-<ordinal>.<serviceName>, and a pool whose governing Service is
      // named differently would otherwise be addressed at a name that does not
      // resolve - which looks like a worker that will not drain.
      val service = Option(sts.getSpec.getServiceName).filter(_.nonEmpty).getOrElse {
        warn(s"StatefulSet $namespace/$statefulSet has no governing Service, " +
          "so its pods have no stable DNS name to drain through")
        return None
      }
      val ordinal = replicas - 1
      Some(DepartingWorker(
        s"$scheme://$statefulSet-$ordinal.$service.$namespace.svc.cluster.local:$port",
        replicas))
    } catch {
      case NonFatal(e) =>
        warn(s"Could not read StatefulSet $namespace/$statefulSet", e)
        None
    }
  }
}
