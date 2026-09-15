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

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model.{ContainerPort, ServicePort}
import io.fabric8.kubernetes.api.model.apps.StatefulSet
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
   *
   * `scheme` and `port` are where to reach a worker when the pool itself does
   * not say where its workers listen.
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
      val (workerScheme, workerPort) = listening(namespace, service, sts, scheme, port)
      Some(DepartingWorker(
        s"$workerScheme://$statefulSet-$ordinal.$service.$namespace.svc.cluster.local:$workerPort",
        replicas))
    } catch {
      case NonFatal(e) =>
        warn(s"Could not read StatefulSet $namespace/$statefulSet", e)
        None
    }
  }

  /**
   * Where the pool's workers listen, as the Service their DNS names go through
   * declares it. `scheme` and `port` stand when that Service cannot be read.
   */
  private def listening(
      namespace: String,
      service: String,
      sts: StatefulSet,
      scheme: String,
      port: Int): (String, Int) = {
    val governing =
      try Option(client.services().inNamespace(namespace).withName(service).get())
      catch {
        case NonFatal(e) =>
          warn(s"Could not read Service $namespace/$service, reaching workers on $scheme:$port", e)
          None
      }
    governing.map { svc =>
      val servicePorts = Option(svc.getSpec).flatMap(s => Option(s.getPorts))
        .map(_.asScala.toSeq).getOrElse(Seq.empty)
      val containerPorts = (for {
        spec <- Option(sts.getSpec)
        template <- Option(spec.getTemplate)
        podSpec <- Option(template.getSpec)
        containers <- Option(podSpec.getContainers)
      } yield containers.asScala.toSeq.flatMap(c =>
        Option(c.getPorts).map(_.asScala.toSeq).getOrElse(Seq.empty))).getOrElse(Seq.empty)
      KubernetesWorkerPool.listening(servicePorts, containerPorts, scheme, port)
    }.getOrElse((scheme, port))
  }
}

object KubernetesWorkerPool {

  private val Schemes = Set("http", "https")

  /**
   * Where workers listen, from the ports of the Service that governs them.
   *
   * A port says what it speaks through `appProtocol` - the field Kubernetes has
   * for exactly this - or, failing that, through its name. A marked port is
   * taken, the one speaking the coordinator's scheme first.
   *
   * Otherwise a Service with a single port still names the port. Workers
   * listening where the coordinator does are taken to be configured like it; a
   * port anywhere else is the plain HTTP one Trino nodes talk to each other on,
   * which is the usual layout behind a coordinator that terminates TLS and
   * authentication itself. With several unmarked ports there is nothing to go
   * on, and the coordinator's scheme and port stand.
   *
   * The pod's port is what counts, not the Service's: a worker's DNS name
   * resolves straight to its pod. A target port given by name is looked up
   * among the pod template's container ports.
   */
  private[scaling] def listening(
      servicePorts: Seq[ServicePort],
      containerPorts: Seq[ContainerPort],
      coordinatorScheme: String,
      coordinatorPort: Int): (String, Int) = {
    def speaks(p: ServicePort): Option[String] =
      Seq(p.getAppProtocol, p.getName).flatMap(Option(_))
        .map(_.trim.toLowerCase(Locale.ROOT)).find(Schemes)

    def podPort(p: ServicePort): Int = {
      val target = Option(p.getTargetPort)
      target.flatMap(t => Option(t.getIntVal)).map(_.intValue).filter(_ > 0)
        .orElse(target.flatMap(t => Option(t.getStrVal))
          .flatMap(name => containerPorts.find(_.getName == name))
          .flatMap(c => Option(c.getContainerPort)).map(_.intValue))
        .orElse(Option(p.getPort).map(_.intValue))
        .getOrElse(coordinatorPort)
    }

    val marked = servicePorts.flatMap(p => speaks(p).map(_ -> p))
    marked.find(_._1 == coordinatorScheme).orElse(marked.headOption) match {
      case Some((scheme, p)) => (scheme, podPort(p))
      case None => servicePorts match {
          case Seq(only) =>
            val at = podPort(only)
            (if (at == coordinatorPort) coordinatorScheme else "http", at)
          case _ => (coordinatorScheme, coordinatorPort)
        }
    }
  }
}
