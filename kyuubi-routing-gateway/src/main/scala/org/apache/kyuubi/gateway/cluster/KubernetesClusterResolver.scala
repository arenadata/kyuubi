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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model.Service
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.gateway.metrics.GatewayMetrics
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.{KubernetesUtils, ThreadUtils}

/**
 * Resolver that learns the cluster set from Kubernetes Services.
 *
 * Only core Services are read - never the operator's custom resources - so this
 * needs no CRD types and no write access. The operator owns the annotations; the
 * gateway only reads them.
 */
class KubernetesClusterResolver
  extends AbstractService("KubernetesClusterResolver") with ClusterResolver {

  import KubernetesClusterResolver._

  private var client: KubernetesClient = _
  private var namespace: Option[String] = None
  private var labelSelector: String = _
  private var pollInterval: Long = _
  private var scaleGroup: String = _
  private var scaleVersion: String = _
  private var scalePlural: String = _

  private val snapshot = new AtomicReference[Seq[ClusterRef]](Seq.empty)

  /** uid -> resourceVersion of the Services the current snapshot was built from. */
  private val seen = new AtomicReference[Map[String, String]](Map.empty)

  private val poller =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("kubernetes-cluster-resolver")

  override def initialize(conf: KyuubiConf): Unit = {
    namespace = conf.getOption(NAMESPACE_KEY).filter(_.nonEmpty)
    labelSelector = conf.getOption(LABEL_SELECTOR_KEY).getOrElse(DEFAULT_LABEL_SELECTOR)
    pollInterval = conf.getOption(POLL_INTERVAL_KEY).map(_.toLong).getOrElse(DEFAULT_POLL_INTERVAL)
    scaleGroup = conf.getOption(SCALE_GROUP_KEY).getOrElse(DEFAULT_SCALE_GROUP)
    scaleVersion = conf.getOption(SCALE_VERSION_KEY).getOrElse(DEFAULT_SCALE_VERSION)
    scalePlural = conf.getOption(SCALE_PLURAL_KEY).getOrElse(DEFAULT_SCALE_PLURAL)
    client = KubernetesUtils.buildKubernetesClient(conf).getOrElse {
      throw new IllegalStateException(
        "Cannot build a Kubernetes client - the gateway cannot discover clusters")
    }
    super.initialize(conf)
  }

  override def start(): Unit = {
    refresh()
    // Published again once the metrics registry exists: the first refresh runs
    // before it, so its result would otherwise be the one set nobody can see.
    GatewayMetrics.publish(snapshot.get())
    poller.scheduleWithFixedDelay(
      () => refresh(),
      pollInterval,
      pollInterval,
      TimeUnit.MILLISECONDS)
    super.start()
  }

  override def stop(): Unit = {
    ThreadUtils.shutdown(poller)
    if (client != null) client.close()
    super.stop()
  }

  override def clusters: Seq[ClusterRef] = snapshot.get()

  override def resolve(user: String, sessionConf: Map[String, String]): Option[ClusterRef] = {
    val current = snapshot.get()
    current.find(_.users.contains(user)).orElse(current.find(_.isDefault))
  }

  /**
   * Rebuilds the snapshot only when something actually changed.
   *
   * Change is detected per object: a uid appearing or disappearing, or a
   * differing resourceVersion. The list-level resourceVersion is deliberately
   * not used - it is the store revision at read time and advances on any change
   * anywhere in the cluster, so it would report a change on nearly every poll.
   */
  private[cluster] def refresh(): Unit = {
    try {
      val services = listServices()
      val current = services
        .map(s => s.getMetadata.getUid -> s.getMetadata.getResourceVersion)
        .toMap
      if (current != seen.get()) {
        val clusters = services.flatMap(toClusterRef)
        snapshot.set(clusters)
        seen.set(current)
        GatewayMetrics.publish(clusters)
        info(s"Cluster set changed, now ${clusters.size}: " +
          clusters.map(c => s"${c.name}->${c.url}").mkString(", "))
      }
    } catch {
      case NonFatal(e) =>
        // Keep serving the last known good snapshot: a transient api-server
        // failure must not make every session unroutable. The counter is how
        // anyone finds out - the published set looks healthy while it is stale.
        GatewayMetrics.refreshFailed()
        warn("Failed to refresh clusters from Kubernetes, keeping previous snapshot", e)
    }
  }

  private def listServices(): Seq[Service] = {
    val filtered = namespace match {
      case Some(ns) => client.services().inNamespace(ns).withLabelSelector(labelSelector)
      case None => client.services().inAnyNamespace().withLabelSelector(labelSelector)
    }
    filtered.list().getItems.asScala.toSeq
  }

  private def toClusterRef(svc: Service): Option[ClusterRef] = {
    val meta = svc.getMetadata
    val ann = Option(meta.getAnnotations).map(_.asScala.toMap).getOrElse(Map.empty)
    ann.get(ENGINE_ANNOTATION).map { engine =>
      val scheme = ann.getOrElse(SCHEME_ANNOTATION, "http")
      val port = resolvePort(svc, ann.get(PORT_ANNOTATION))
      val host = s"${meta.getName}.${meta.getNamespace}"
      ClusterRef(
        name = s"${meta.getNamespace}/${meta.getName}",
        engine = engine,
        url = s"$scheme://$host:$port",
        users = ann.get(USERS_ANNOTATION).map(csv).getOrElse(Set.empty),
        isDefault = ann.get(DEFAULT_ANNOTATION).exists(_.toBoolean),
        capacity = declaredCapacity(ann),
        scaleTarget = scaleTarget(meta.getNamespace, ann),
        sessionConf = ann.collect {
          case (k, v) if k.startsWith(SESSION_ANNOTATION_PREFIX) =>
            k.substring(SESSION_ANNOTATION_PREFIX.length) -> v
        })
    }
  }

  private def resolvePort(svc: Service, requested: Option[String]): Int = {
    val ports = Option(svc.getSpec).map(_.getPorts.asScala.toSeq).getOrElse(Seq.empty)
    requested match {
      case Some(p) if p.forall(_.isDigit) => p.toInt
      case Some(name) => ports.find(_.getName == name).map(_.getPort.intValue()).getOrElse(
          throw new IllegalArgumentException(
            s"Service ${svc.getMetadata.getName} has no port named $name"))
      case None => ports.headOption.map(_.getPort.intValue()).getOrElse(DEFAULT_PORT)
    }
  }

  /**
   * Capacity is only reported when all three parts are present and sensible.
   *
   * A partial declaration is worse than none: the accountant would size against
   * a made-up ceiling and either refuse queries that fit or admit ones that do
   * not. Absent capacity is a state the caller already has to handle.
   */
  private def declaredCapacity(ann: Map[String, String]): Option[DeclaredCapacity] =
    for {
      memory <- ann.get(MAX_MEMORY_PER_NODE_ANNOTATION).flatMap(parsePositiveLong)
      workers <- ann.get(WORKERS_ANNOTATION).flatMap(parsePositiveLong).map(_.toInt)
      maxWorkers <- ann.get(MAX_WORKERS_ANNOTATION).flatMap(parsePositiveLong).map(_.toInt)
      if maxWorkers >= workers
    } yield DeclaredCapacity(memory, workers, maxWorkers)

  /**
   * The resource whose replica count stands for this cluster's size.
   *
   * Named by annotation rather than derived from an owner reference: the
   * gateway is told what it may scale, so a Service it happens to reach cannot
   * hand it write access to an object nobody meant to expose.
   */
  private def scaleTarget(namespace: String, ann: Map[String, String]): Option[ScaleTarget] =
    ann.get(SCALE_TARGET_ANNOTATION).map(_.trim).filter(_.nonEmpty).map { name =>
      ScaleTarget(
        namespace = namespace,
        name = name,
        group = ann.getOrElse(SCALE_GROUP_ANNOTATION, scaleGroup),
        version = ann.getOrElse(SCALE_VERSION_ANNOTATION, scaleVersion),
        plural = ann.getOrElse(SCALE_PLURAL_ANNOTATION, scalePlural))
    }

  private def parsePositiveLong(s: String): Option[Long] =
    try {
      val v = s.trim.toLong
      if (v > 0) Some(v) else None
    } catch {
      case _: NumberFormatException =>
        warn(s"Ignoring a capacity annotation that is not a number: $s")
        None
    }

  private def csv(s: String): Set[String] =
    s.split(",").map(_.trim).filter(_.nonEmpty).toSet
}

object KubernetesClusterResolver {

  val NAMESPACE_KEY = "kyuubi.gateway.kubernetes.namespace"
  val LABEL_SELECTOR_KEY = "kyuubi.gateway.kubernetes.labelSelector"
  val POLL_INTERVAL_KEY = "kyuubi.gateway.kubernetes.pollInterval"

  val DEFAULT_LABEL_SELECTOR = "kyuubi.gateway/enabled=true"
  val DEFAULT_POLL_INTERVAL = 10000L
  val DEFAULT_PORT = 8080

  val ENGINE_ANNOTATION = "kyuubi.gateway/engine"
  val SCHEME_ANNOTATION = "kyuubi.gateway/scheme"
  val PORT_ANNOTATION = "kyuubi.gateway/port"
  val USERS_ANNOTATION = "kyuubi.gateway/users"
  val DEFAULT_ANNOTATION = "kyuubi.gateway/default"
  val SESSION_ANNOTATION_PREFIX = "kyuubi.gateway/session."
  val MAX_MEMORY_PER_NODE_ANNOTATION = "kyuubi.gateway/max-memory-per-node-bytes"
  val WORKERS_ANNOTATION = "kyuubi.gateway/workers"
  val MAX_WORKERS_ANNOTATION = "kyuubi.gateway/max-workers"

  /** Name of the custom resource holding this cluster's worker count. */
  val SCALE_TARGET_ANNOTATION = "kyuubi.gateway/scale-target"
  val SCALE_GROUP_ANNOTATION = "kyuubi.gateway/scale-group"
  val SCALE_VERSION_ANNOTATION = "kyuubi.gateway/scale-version"
  val SCALE_PLURAL_ANNOTATION = "kyuubi.gateway/scale-plural"

  val SCALE_GROUP_KEY = "kyuubi.gateway.kubernetes.scale.group"
  val SCALE_VERSION_KEY = "kyuubi.gateway.kubernetes.scale.version"
  val SCALE_PLURAL_KEY = "kyuubi.gateway.kubernetes.scale.plural"

  // Defaults describe the Trino operator's Cluster CRD.
  val DEFAULT_SCALE_GROUP = "trino.arenadata.io"
  val DEFAULT_SCALE_VERSION = "v1alpha1"
  val DEFAULT_SCALE_PLURAL = "clusters"
}
