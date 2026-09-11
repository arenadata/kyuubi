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
import org.apache.kyuubi.engine.jdbc.operation.JdbcOperationManager
import org.apache.kyuubi.engine.jdbc.session.JdbcSessionImpl
import org.apache.kyuubi.gateway.cluster.{ClusterRef, ClusterResolver}
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

/**
 * Routes to any JDBC-reachable engine. Impala is the case this was added for -
 * `ImpalaDialect` ships with the engine, so no dialect work is needed here.
 *
 * The dialect is picked from `kyuubi.engine.jdbc.type` when the cluster sets it,
 * otherwise from the url scheme: `jdbc:impala://...` resolves to impala.
 */
class JdbcRoutingSessionManager(resolver: ClusterResolver, override val engine: String)
  extends RoutingSessionManager("JdbcRoutingSessionManager", resolver) {

  // lazy: JdbcOperationManager needs conf, which is only set during initialize
  override lazy val operationManager: OperationManager = new JdbcOperationManager(conf)

  override protected def connectionConf(cluster: ClusterRef): Map[String, String] =
    Map(
      KyuubiConf.ENGINE_JDBC_CONNECTION_URL.key -> cluster.url,
      KyuubiConf.ENGINE_JDBC_SHORT_NAME.key -> engine)

  override protected def createEngineSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      cluster: ClusterRef): Session =
    // Admission is not wired for the JDBC path yet: Impala does its own
    // admission control, so double-accounting it needs thought rather than a
    // copy of the Trino branch.
    new JdbcSessionImpl(protocol, user, password, ipAddress, conf, this)
}
