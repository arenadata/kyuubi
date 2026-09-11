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

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.operation.TrinoOperationManager
import org.apache.kyuubi.engine.trino.session.TrinoSessionImpl
import org.apache.kyuubi.gateway.cluster.ClusterResolver
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.{Session, SessionManager}

/**
 * Session manager that resolves the target cluster per authenticated user and
 * hands the resolved address to the engine session layer through session
 * configuration.
 *
 * This is the whole of the routing change: the Trino session layer is reused
 * unmodified, because [[TrinoSessionImpl]] reads the connection url from the
 * session conf rather than from a fixed engine configuration.
 *
 * Deliberately extends [[SessionManager]] and not `TrinoSessionManager`: the
 * latter stops the JVM when the last session closes at CONNECTION share level,
 * which is correct for an engine process and wrong for a long-lived gateway.
 */
class RoutingSessionManager(resolver: ClusterResolver)
  extends SessionManager("RoutingSessionManager") {

  override val operationManager: OperationManager = new TrinoOperationManager()

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
    if (cluster.engine != "trino") {
      throw KyuubiSQLException(
        s"Cluster ${cluster.name} has engine ${cluster.engine}, only trino is supported yet")
    }
    val routed = conf ++
      cluster.sessionConf +
      (KyuubiConf.ENGINE_TRINO_CONNECTION_URL.key -> cluster.url)
    info(s"Opening session for $user on cluster ${cluster.name} at ${cluster.url}")
    new TrinoSessionImpl(protocol, user, password, ipAddress, routed, this)
  }
}
