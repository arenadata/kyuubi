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

package org.apache.kyuubi.gateway.cluster

/**
 * A backend cluster the gateway can route a session to.
 *
 * @param name        cluster identity, unique within the gateway
 * @param engine      engine type, currently only "trino"
 * @param url         address the engine client connects to
 * @param users       users allowed here; empty means "only reachable as default"
 * @param isDefault   whether users with no explicit mapping land here
 * @param sessionConf extra session configuration applied to sessions routed here
 * @param capacity    what the cluster can hold, when the operator declared it
 * @param scaleTarget the object whose worker count the gateway may raise
 */
case class ClusterRef(
    name: String,
    engine: String,
    url: String,
    users: Set[String] = Set.empty,
    isDefault: Boolean = false,
    sessionConf: Map[String, String] = Map.empty,
    capacity: Option[DeclaredCapacity] = None,
    scaleTarget: Option[ScaleTarget] = None)

/**
 * The custom resource whose `scale` subresource stands for a cluster's size.
 *
 * Addressed by group, version and plural rather than by a typed client so the
 * gateway carries no operator types and works against any CRD that declares
 * `scale`. Where the replica count actually lives in the spec is the CRD's
 * business, declared once in its `specReplicasPath`, and deliberately not
 * something the gateway is told or could get wrong.
 */
case class ScaleTarget(
    namespace: String,
    name: String,
    group: String,
    version: String,
    plural: String,
    /**
     * The StatefulSet the workers run as, when shrinking is wanted.
     *
     * Named rather than discovered for the same reason as the scale target
     * itself, and absent when the pool is a Deployment - which cannot be shrunk
     * safely, because Kubernetes rather than the gateway chooses which pod goes.
     */
    workerStatefulSet: Option[String] = None) {

  override def toString: String = s"$plural.$group/$namespace/$name"
}

/**
 * Capacity as declared alongside the cluster, not measured.
 *
 * The operator knows these because it writes the engine's configuration and
 * owns the scaling bounds; the gateway would otherwise have to guess them or
 * scrape them. `workers` is the count the operator last published, which lags
 * reality between updates - a resolver with a live worker count should override
 * it rather than trust it.
 */
case class DeclaredCapacity(
    maxMemoryPerNodeBytes: Long,
    workers: Int,
    maxWorkers: Int,
    /**
     * The floor a scale-down will not go below. Never zero: a cluster with no
     * workers cannot answer the query that would bring it back.
     */
    minWorkers: Int = 1)

/**
 * Resolves the cluster a session belongs to.
 *
 * Resolution happens once per session, at open time: the routing key is the
 * authenticated identity, which is constant for the life of the session.
 */
trait ClusterResolver {

  /** All clusters currently known to the resolver. */
  def clusters: Seq[ClusterRef]

  /**
   * Picks the cluster for a session. Returning None means the user is not
   * allowed anywhere and the session must be rejected - never silently
   * redirected to some default, which would turn a routing mistake into an
   * access control bypass.
   */
  def resolve(user: String, sessionConf: Map[String, String]): Option[ClusterRef]
}
