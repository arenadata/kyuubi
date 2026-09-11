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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.operation.TrinoOperationManager
import org.apache.kyuubi.engine.trino.session.TrinoSessionImpl
import org.apache.kyuubi.gateway.capacity.AdmissionGate
import org.apache.kyuubi.gateway.cluster.{ClusterRef, ClusterResolver}
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class TrinoRoutingSessionManager(resolver: ClusterResolver, gate: Option[AdmissionGate] = None)
  extends RoutingSessionManager("TrinoRoutingSessionManager", resolver) {

  override val engine: String = "trino"

  override val operationManager: OperationManager = new TrinoOperationManager()

  override protected def connectionConf(cluster: ClusterRef): Map[String, String] =
    Map(KyuubiConf.ENGINE_TRINO_CONNECTION_URL.key -> cluster.url)

  override protected def createEngineSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      cluster: ClusterRef): Session = gate match {
    case Some(g) =>
      new GatedTrinoSession(protocol, user, password, ipAddress, conf, this, g, cluster)
    case None =>
      // Without a gate the gateway still routes; it just does not account for
      // what it lets through. Useful for a first rollout, where routing is the
      // change being proven and capacity is still managed by hand.
      new TrinoSessionImpl(protocol, user, password, ipAddress, conf, this)
  }
}
