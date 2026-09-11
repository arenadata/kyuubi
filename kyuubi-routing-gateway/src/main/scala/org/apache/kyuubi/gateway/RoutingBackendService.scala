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
import org.apache.kyuubi.gateway.capacity.AdmissionGate
import org.apache.kyuubi.gateway.capacity.AdmissionPolicy
import org.apache.kyuubi.gateway.capacity.CapacityAccountant
import org.apache.kyuubi.gateway.capacity.ClusterCapacity
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.gateway.cluster.ClusterResolver
import org.apache.kyuubi.gateway.cluster.KubernetesClusterResolver
import org.apache.kyuubi.gateway.cluster.StaticClusterResolver
import org.apache.kyuubi.gateway.session.JdbcRoutingSessionManager
import org.apache.kyuubi.gateway.session.RoutingSessionManager
import org.apache.kyuubi.gateway.session.TrinoRoutingSessionManager
import org.apache.kyuubi.gateway.sizing.QuerySizer
import org.apache.kyuubi.gateway.sizing.SizingPolicy
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
    _sessionManager = RoutingBackendService.sessionManagerFor(conf, resolver)
    super.initialize(conf)
  }
}

object RoutingBackendService {

  val RESOLVER_KEY = "kyuubi.gateway.cluster.resolver"
  val ENGINE_KEY = "kyuubi.gateway.engine"

  /**
   * One gateway instance serves one engine - see RoutingSessionManager for why
   * a single instance cannot dispatch between engine-specific operation
   * managers.
   */
  def sessionManagerFor(conf: KyuubiConf, resolver: ClusterResolver): RoutingSessionManager = {
    conf.getOption(ENGINE_KEY).getOrElse("trino").toLowerCase match {
      case "trino" => new TrinoRoutingSessionManager(resolver, gateFor(conf))
      case jdbcEngine => new JdbcRoutingSessionManager(resolver, jdbcEngine)
    }
  }

  val ADMISSION_ENABLED_KEY = "kyuubi.gateway.admission.enabled"
  val ADMISSION_POLICY_KEY = "kyuubi.gateway.admission.policy"
  val MEMORY_FACTOR_KEY = "kyuubi.gateway.sizing.memoryFactor"
  val DEFAULT_WORKERS_KEY = "kyuubi.gateway.sizing.defaultWorkers"

  /**
   * Builds the admission gate, or none.
   *
   * Off by default: routing is useful on its own, and admission changes what
   * clients see - a query that used to run can now be refused. Turning it on
   * should be a decision, not something that arrives with an upgrade.
   */
  def gateFor(conf: KyuubiConf): Option[AdmissionGate] = {
    if (!conf.getOption(ADMISSION_ENABLED_KEY).exists(_.toBoolean)) return None

    val policy = conf.getOption(ADMISSION_POLICY_KEY).getOrElse("PackByMemory") match {
      case p if p.equalsIgnoreCase("Exclusive") => AdmissionPolicy.Exclusive
      case _ => AdmissionPolicy.PackByMemory
    }
    val sizing = SizingPolicy(
      memoryFactor = conf.getOption(MEMORY_FACTOR_KEY).map(_.toDouble).getOrElse(1.5),
      defaultWorkers = conf.getOption(DEFAULT_WORKERS_KEY).map(_.toInt).getOrElse(2))

    Some(new AdmissionGate(
      new CapacityAccountant(policy),
      new QuerySizer(sizing),
      capacityOf,
      // EXPLAIN is not wired to a client yet, so every statement sizes as
      // unknown and lands on defaultWorkers. Accounting is real from the start;
      // only its precision waits on this.
      (_, _) => ""))
  }

  private def capacityOf(cluster: ClusterRef): Option[ClusterCapacity] =
    cluster.capacity.map(c =>
      ClusterCapacity(c.maxMemoryPerNodeBytes, c.workers, c.maxWorkers))

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
