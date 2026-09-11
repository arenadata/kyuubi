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

package org.apache.kyuubi.gateway.session

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.gateway.cluster.{ClusterRef, ClusterResolver}
import org.apache.kyuubi.session.{Session, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

/**
 * Resolves the target cluster per authenticated user and hands its address to
 * the engine session layer through session configuration.
 *
 * This is the whole of the routing change: the engine session layers read their
 * connection address from session conf, so routing only has to put the right
 * address there. They are reused unmodified.
 *
 * Deliberately extends [[SessionManager]] and not the engines' own session
 * managers: those stop the JVM when the last session closes at CONNECTION share
 * level, which is right for an engine process and wrong for a gateway.
 *
 * One instance serves one engine type. `OperationManager.addOperation` and
 * `getOperation` are final and keep their own registry, and
 * `AbstractBackendService` looks operations up through
 * `sessionManager.operationManager` - so a single instance cannot dispatch
 * between engine-specific operation managers without reimplementing every
 * operation factory. Running one gateway per engine costs a deployment and
 * keeps the engine-specific paths apart, including the capacity accounting,
 * which differs between them anyway.
 */
abstract class RoutingSessionManager(name: String, resolver: ClusterResolver)
  extends SessionManager(name) {

  /** Engine type this instance serves; clusters of other engines are refused. */
  def engine: String

  override protected def isServer: Boolean = true

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    val cluster = resolver.resolve(user, conf).getOrElse {
      throw KyuubiSQLException(s"No cluster is allowed for user $user")
    }
    if (!cluster.engine.equalsIgnoreCase(engine)) {
      throw KyuubiSQLException(
        s"Cluster ${cluster.name} runs ${cluster.engine}, this gateway serves $engine")
    }
    // The engine session layers read the identity to present to the cluster from
    // this key, and fall back to the OS user of the current process when it is
    // absent. In stock Kyuubi the process builders set it when launching a
    // per-user engine; the gateway has no such launch, so setting it here is
    // what keeps a query arriving at the cluster as its caller instead of as the
    // gateway's own service account. Placed after the cluster conf so that no
    // per-cluster setting can override the authenticated user.
    val routed = conf ++ cluster.sessionConf ++ connectionConf(cluster) +
      (KYUUBI_SESSION_USER_KEY -> user)
    info(s"Opening $engine session for $user on ${cluster.name} at ${cluster.url}")
    createEngineSession(protocol, user, password, ipAddress, routed, cluster)
  }

  /** Session configuration that points the engine session layer at the cluster. */
  protected def connectionConf(cluster: ClusterRef): Map[String, String]

  /**
   * Builds the engine session.
   *
   * The cluster is passed rather than left to be re-derived: admission is
   * accounted per cluster, and a session that disagreed with the routing
   * decision about which cluster it is on would corrupt that accounting.
   */
  protected def createEngineSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      cluster: ClusterRef): Session
}
