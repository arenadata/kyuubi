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

import okhttp3.OkHttpClient

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect
import org.apache.kyuubi.gateway.capacity.AdmissionGate
import org.apache.kyuubi.gateway.capacity.AdmissionPolicy
import org.apache.kyuubi.gateway.capacity.CapacityAccountant
import org.apache.kyuubi.gateway.capacity.ClusterCapacity
import org.apache.kyuubi.gateway.capacity.InMemoryReservationStore
import org.apache.kyuubi.gateway.capacity.MemoryCalibration
import org.apache.kyuubi.gateway.capacity.ReservationReconciler
import org.apache.kyuubi.gateway.capacity.ReservationStore
import org.apache.kyuubi.gateway.capacity.SecretReservationStore
import org.apache.kyuubi.gateway.capacity.TrinoClusterQueries
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.gateway.cluster.ClusterResolver
import org.apache.kyuubi.gateway.cluster.KubernetesClusterResolver
import org.apache.kyuubi.gateway.cluster.StaticClusterResolver
import org.apache.kyuubi.gateway.metrics.GatewayMetrics
import org.apache.kyuubi.gateway.scaling.ClusterScaler
import org.apache.kyuubi.gateway.scaling.ClusterShrinker
import org.apache.kyuubi.gateway.scaling.FabricScaleApi
import org.apache.kyuubi.gateway.scaling.KubernetesClusterScaler
import org.apache.kyuubi.gateway.scaling.KubernetesWorkerPool
import org.apache.kyuubi.gateway.scaling.TrinoWorkerDrain
import org.apache.kyuubi.gateway.session.JdbcRoutingSessionManager
import org.apache.kyuubi.gateway.session.RoutingSessionManager
import org.apache.kyuubi.gateway.session.TrinoRoutingSessionManager
import org.apache.kyuubi.gateway.sizing.QuerySizer
import org.apache.kyuubi.gateway.sizing.SizingPolicy
import org.apache.kyuubi.service.{AbstractBackendService, Service}
import org.apache.kyuubi.session.SessionManager
import org.apache.kyuubi.util.KubernetesUtils
import org.apache.kyuubi.util.reflect.ReflectUtils.loadFromServiceLoader

/**
 * Backend service of the gateway. Unlike the Kyuubi server it launches nothing:
 * sessions are opened directly against clusters that already exist, so there is
 * exactly one intermediary process between the client and the engine.
 */
class RoutingBackendService(resolverFactory: KyuubiConf => ClusterResolver)
  extends AbstractBackendService("RoutingBackendService") {

  def this() = this(RoutingBackendService.resolverFor)

  @volatile private var _sessionManager: RoutingSessionManager = _
  @volatile private var _resolver: ClusterResolver = _

  override def sessionManager: SessionManager = _sessionManager

  override def initialize(conf: KyuubiConf): Unit = {
    val resolver = resolverFactory(conf)
    // A resolver may have its own lifecycle - the Kubernetes one polls - so it
    // is registered as a child service when it has one.
    resolver match {
      case service: Service => addService(service)
      case _ =>
    }
    val accountant = RoutingBackendService.accountantFor(conf)
    // The session manager must exist before super.initialize, which registers
    // it as a child service itself - registering it here too would double-add.
    _sessionManager = RoutingBackendService.sessionManagerFor(conf, resolver, accountant)
    accountant.foreach { a =>
      RoutingBackendService.reconcilerFor(conf, a, resolver).foreach(addService)
      RoutingBackendService.shrinkerFor(conf, a, resolver).foreach(addService)
    }
    _resolver = resolver
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    // After start, because the metrics registry only exists from then on. A
    // resolver that polls publishes on its own; this is what puts a static one
    // - which never changes and so never publishes - into the metrics at all.
    GatewayMetrics.publish(_resolver.clusters)
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
  def sessionManagerFor(
      conf: KyuubiConf,
      resolver: ClusterResolver,
      accountant: Option[CapacityAccountant]): RoutingSessionManager = {
    conf.getOption(ENGINE_KEY).getOrElse("trino").toLowerCase match {
      case "trino" => new TrinoRoutingSessionManager(resolver, gateFor(conf, accountant))
      case jdbc if jdbcEngines.contains(jdbc) => new JdbcRoutingSessionManager(resolver, jdbc)
      case other =>
        // Checked here rather than left to the dialect lookup, which happens at
        // the first session and reports "Don't find jdbc dialect implement for
        // jdbc engine: spark" - a message about an internal lookup, from a
        // gateway that came up and reported healthy. A name this gateway cannot
        // serve is a startup error, the same as a frontend protocol it does not
        // implement.
        throw new IllegalArgumentException(
          s"$ENGINE_KEY is '$other', which this gateway cannot serve. " +
            s"Supported: trino, ${jdbcEngines.toSeq.sorted.mkString(", ")}")
    }
  }

  /**
   * Engines reachable over JDBC, as the dialects on the classpath declare them.
   *
   * Read from the dialects rather than listed here, so the two cannot disagree:
   * adding a dialect is what makes an engine available, and this follows.
   */
  private[gateway] def jdbcEngines: Set[String] =
    loadFromServiceLoader[JdbcDialect]().map(_.name().toLowerCase).toSet

  /**
   * The one accountant, or none when admission is off.
   *
   * Built here rather than inside the gate because the reconciler has to
   * release from the same ledger the gate admits into - two instances would
   * agree on nothing.
   */
  def accountantFor(conf: KyuubiConf): Option[CapacityAccountant] = {
    if (!conf.getOption(ADMISSION_ENABLED_KEY).exists(_.toBoolean)) return None
    val policy = conf.getOption(ADMISSION_POLICY_KEY).getOrElse("PackByMemory") match {
      case p if p.equalsIgnoreCase("Exclusive") => AdmissionPolicy.Exclusive
      case _ => AdmissionPolicy.PackByMemory
    }
    Some(new CapacityAccountant(policy, storeFor(conf)))
  }

  /**
   * Where reservations are kept.
   *
   * In this process unless told otherwise, because a single replica is the
   * common deployment and a shared ledger costs an api-server round trip per
   * admission. Sharing must be switched on deliberately - but running more than
   * one replica without it means each admits against its own ledger and
   * together they overcommit every cluster, so the choice belongs with whoever
   * decided to scale the gateway out.
   */
  def storeFor(conf: KyuubiConf): ReservationStore = {
    if (!conf.getOption(SecretReservationStore.ENABLED_KEY).exists(_.toBoolean)) {
      return new InMemoryReservationStore
    }
    val client = KubernetesUtils.buildKubernetesClient(conf).getOrElse {
      throw new IllegalStateException(
        s"${SecretReservationStore.ENABLED_KEY} is on but no Kubernetes client could be built")
    }
    val namespace = conf.getOption(SecretReservationStore.NAMESPACE_KEY)
      .orElse(Option(client.getNamespace))
      .filter(_.nonEmpty)
      .getOrElse(throw new IllegalStateException(
        s"${SecretReservationStore.NAMESPACE_KEY} must be set: the shared ledger needs a " +
          "namespace to live in and none could be inferred"))
    new SecretReservationStore(
      client,
      namespace,
      pollMillis = conf.getOption(SecretReservationStore.POLL_KEY).map(_.toLong).getOrElse(1000L))
  }

  /**
   * Builds the reconciler, or none.
   *
   * Separate from admission because it talks to the coordinator's REST endpoint
   * as a particular user, which a secured cluster may not allow - and a gateway
   * that cannot ask should say so rather than half-work.
   */
  def reconcilerFor(
      conf: KyuubiConf,
      accountant: CapacityAccountant,
      resolver: ClusterResolver): Option[ReservationReconciler] = {
    if (!conf.getOption(ReservationReconciler.ENABLED_KEY).exists(_.toBoolean)) return None
    val user = conf.getOption(ReservationReconciler.USER_KEY)
      .getOrElse(org.apache.kyuubi.Utils.currentUser)
    Some(new ReservationReconciler(
      accountant,
      resolver,
      new TrinoClusterQueries(new OkHttpClient.Builder().build(), user),
      Some(new MemoryCalibration),
      ReservationReconciler.graceFrom(conf),
      ReservationReconciler.intervalFrom(conf)))
  }

  val ADMISSION_ENABLED_KEY = "kyuubi.gateway.admission.enabled"
  val ADMISSION_POLICY_KEY = "kyuubi.gateway.admission.policy"
  val SCALING_ENABLED_KEY = "kyuubi.gateway.scaling.enabled"
  val HOLD_TIMEOUT_KEY = "kyuubi.gateway.admission.holdTimeout"
  val MEMORY_FACTOR_KEY = "kyuubi.gateway.sizing.memoryFactor"
  val DEFAULT_WORKERS_KEY = "kyuubi.gateway.sizing.defaultWorkers"

  /**
   * Builds the admission gate, or none.
   *
   * Off by default: routing is useful on its own, and admission changes what
   * clients see - a query that used to run can now be refused. Turning it on
   * should be a decision, not something that arrives with an upgrade.
   */
  def gateFor(conf: KyuubiConf, accountant: Option[CapacityAccountant]): Option[AdmissionGate] = {
    val ledger = accountant.getOrElse(return None)

    val sizing = SizingPolicy(
      memoryFactor = conf.getOption(MEMORY_FACTOR_KEY).map(_.toDouble).getOrElse(1.5),
      defaultWorkers = conf.getOption(DEFAULT_WORKERS_KEY).map(_.toInt).getOrElse(2))

    Some(new AdmissionGate(
      ledger,
      new QuerySizer(sizing),
      capacityOf,
      scalerFor(conf),
      conf.getOption(HOLD_TIMEOUT_KEY).map(_.toLong).getOrElse(0L)))
  }

  /**
   * Builds the scaler, or none.
   *
   * Off by default and separately from admission: admission only refuses, while
   * scaling writes to Kubernetes and costs money. A cluster that is sized by
   * hand or by something else entirely must not start growing because the gate
   * was switched on.
   */
  def scalerFor(conf: KyuubiConf): Option[ClusterScaler] = {
    if (!conf.getOption(SCALING_ENABLED_KEY).exists(_.toBoolean)) return None
    val client = KubernetesUtils.buildKubernetesClient(conf).getOrElse {
      throw new IllegalStateException(
        s"$SCALING_ENABLED_KEY is on but no Kubernetes client could be built")
    }
    Some(new KubernetesClusterScaler(new FabricScaleApi(client)))
  }

  /**
   * Builds the shrinker, or none.
   *
   * Separate from scaling up, and off by default even when that is on: growing
   * a cluster costs money and shrinking one can cost a query. It also needs
   * more than scaling does - the worker StatefulSet named on each cluster, and
   * read access to StatefulSets - so a deployment that has not arranged those
   * should not find itself shrinking.
   */
  def shrinkerFor(
      conf: KyuubiConf,
      accountant: CapacityAccountant,
      resolver: ClusterResolver): Option[ClusterShrinker] = {
    if (!conf.getOption(ClusterShrinker.ENABLED_KEY).exists(_.toBoolean)) return None
    val client = KubernetesUtils.buildKubernetesClient(conf).getOrElse {
      throw new IllegalStateException(
        s"${ClusterShrinker.ENABLED_KEY} is on but no Kubernetes client could be built")
    }
    Some(new ClusterShrinker(
      accountant,
      resolver,
      new KubernetesWorkerPool(client),
      new TrinoWorkerDrain(new OkHttpClient.Builder().build()),
      new FabricScaleApi(client),
      capacityOf,
      ClusterShrinker.idleAfterFrom(conf),
      ClusterShrinker.intervalFrom(conf),
      ClusterShrinker.drainTimeoutFrom(conf)))
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
