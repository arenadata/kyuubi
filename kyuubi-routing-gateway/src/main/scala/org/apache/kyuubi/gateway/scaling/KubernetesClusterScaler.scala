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
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.base.PatchContext
import io.fabric8.kubernetes.client.dsl.base.PatchType
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext
import io.fabric8.kubernetes.client.utils.Serialization

import org.apache.kyuubi.Logging
import org.apache.kyuubi.gateway.cluster.ScaleTarget

/**
 * Raises the replica count in a custom resource's spec.
 *
 * A merge patch of the one field, not a replacement of the object: the operator
 * and whoever else edits the resource must keep their changes. The path is
 * taken from the target, so the gateway holds no operator types and a different
 * CRD needs configuration rather than code.
 *
 * The current size is read from the resource itself rather than remembered.
 * A remembered size goes stale the moment anything else scales the cluster, and
 * acting on a stale one means either a scale-up that never happens or a query
 * admitted against workers that are not there.
 */
class KubernetesClusterScaler(client: KubernetesClient) extends ClusterScaler with Logging {

  override def ensureAtLeast(target: ScaleTarget, workers: Int): Int = {
    try {
      val resource = get(target)
      val current = replicasOf(resource, target.replicasPath)
      current match {
        case Some(size) if size >= workers =>
          debug(s"$target already has $size workers, no scale-up needed for $workers")
          size
        case _ =>
          patchReplicas(target, workers)
          info(s"Asked $target for $workers workers, was ${current.getOrElse("unset")}")
          workers
      }
    } catch {
      case NonFatal(e) =>
        // Report what exists rather than what was asked for: the caller sizes
        // an admission against the answer, and claiming workers that were never
        // requested would admit a query onto a cluster that cannot hold it.
        warn(s"Could not scale $target to $workers workers", e)
        0
    }
  }

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

  private def get(target: ScaleTarget): GenericKubernetesResource = {
    val found = resource(target).get()
    if (found == null) {
      throw new IllegalStateException(s"$target does not exist")
    }
    found
  }

  private def patchReplicas(target: ScaleTarget, workers: Int): Unit = {
    resource(target)
      .patch(
        PatchContext.of(PatchType.JSON_MERGE),
        Serialization.asJson(KubernetesClusterScaler.nest(target.replicasPath, workers)))
  }

  private def replicasOf(
      resource: GenericKubernetesResource,
      path: Seq[String]): Option[Int] =
    KubernetesClusterScaler.dig(resource.getAdditionalProperties.asScala.toMap, path)
}

object KubernetesClusterScaler {

  /** Builds `{"spec":{"worker":{"replicas":n}}}` from the path and the count. */
  def nest(path: Seq[String], value: Int): java.util.Map[String, Any] =
    path.foldRight[Any](value) { (segment, inner) =>
      Map(segment -> inner)
    } match {
      case m: Map[_, _] => m.asInstanceOf[Map[String, Any]].asJava
      case other =>
        throw new IllegalArgumentException(s"A replicas path cannot be empty, got $other")
    }

  /**
   * Follows the path through the parsed resource.
   *
   * Returns None for an absent or non-numeric value rather than throwing: a
   * spec that has never had a replica count set is an ordinary state, and means
   * the same thing to the caller as "fewer than you need".
   */
  def dig(root: Map[String, Any], path: Seq[String]): Option[Int] = path.toList match {
    case Nil => None
    case last :: Nil =>
      root.get(last).flatMap {
        case n: Number => Some(n.intValue())
        case s: String => scala.util.Try(s.trim.toInt).toOption
        case _ => None
      }
    case head :: rest =>
      root.get(head).flatMap {
        case m: java.util.Map[_, _] =>
          dig(m.asInstanceOf[java.util.Map[String, Any]].asScala.toMap, rest)
        case m: Map[_, _] => dig(m.asInstanceOf[Map[String, Any]], rest)
        case _ => None
      }
  }
}
