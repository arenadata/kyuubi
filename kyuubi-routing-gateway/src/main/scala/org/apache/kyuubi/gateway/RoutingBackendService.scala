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

package org.apache.kyuubi.gateway

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.gateway.cluster.{ClusterResolver, KubernetesClusterResolver, StaticClusterResolver}
import org.apache.kyuubi.gateway.session.RoutingSessionManager
import org.apache.kyuubi.service.{AbstractBackendService, Service}
import org.apache.kyuubi.session.SessionManager

/**
 * Backend service of the gateway. Unlike the Kyuubi server it launches nothing:
 * sessions are opened directly against clusters that already exist, so there is
 * exactly one intermediary process between the client and the engine.
 */
class RoutingBackendService(resolverFactory: KyuubiConf => ClusterResolver)
  extends AbstractBackendService("RoutingBackendService") {

  def this() = this(RoutingBackendService.resolverFor)

  @volatile private var _sessionManager: RoutingSessionManager = _

  override def sessionManager: SessionManager = _sessionManager

  override def initialize(conf: KyuubiConf): Unit = {
    val resolver = resolverFactory(conf)
    // A resolver may have its own lifecycle - the Kubernetes one polls - so it
    // is registered as a child service when it has one.
    resolver match {
      case service: Service => addService(service)
      case _ =>
    }
    // The session manager must exist before super.initialize, which registers
    // it as a child service itself - registering it here too would double-add.
    _sessionManager = new RoutingSessionManager(resolver)
    super.initialize(conf)
  }
}

object RoutingBackendService {

  val RESOLVER_KEY = "kyuubi.gateway.cluster.resolver"

  def resolverFor(conf: KyuubiConf): ClusterResolver = {
    conf.getOption(RESOLVER_KEY).getOrElse("static").toLowerCase match {
      case "static" => new StaticClusterResolver(conf.getAll)
      case "kubernetes" | "k8s" => new KubernetesClusterResolver()
      case other =>
        throw new IllegalArgumentException(
          s"Unknown $RESOLVER_KEY value '$other', expected 'static' or 'kubernetes'")
    }
  }
}
