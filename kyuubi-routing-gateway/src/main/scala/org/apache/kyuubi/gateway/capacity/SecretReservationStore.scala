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

package org.apache.kyuubi.gateway.capacity

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.{Base64, Locale}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import io.fabric8.kubernetes.api.model.{Secret, SecretBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException}

import org.apache.kyuubi.Logging

/**
 * Reservations shared between gateway replicas, in a Secret per cluster.
 *
 * Several replicas admitting from separate ledgers each see only their own
 * reservations and together admit more than the cluster can hold - exactly the
 * overcommit the gate exists to prevent. They need one ledger.
 *
 * Kubernetes is used as the store rather than something added for the purpose
 * because it is already there, already the source of truth for which clusters
 * exist, and offers the one primitive this needs: a compare-and-swap. A write
 * carries the resourceVersion it was based on and is rejected if anything
 * changed meanwhile, which turns read-modify-write into an atomic operation
 * without a lock and therefore without anything to release when a replica dies.
 *
 * A Secret rather than a ConfigMap because the ledger is not public
 * information: it says which clusters are busy and how much is running on each,
 * which is a map of the platform's load, and Secrets are what RBAC and
 * encryption-at-rest are usually already configured for.
 *
 * Deliberately not leader election. A leader would make every admission wait
 * for one replica and make its death an outage; a compare-and-swap lets every
 * replica admit and lets the loser retry.
 *
 * One Secret per cluster, not one for everything: admissions on different
 * clusters have no reason to contend, and a single object would serialise the
 * whole gateway behind whichever cluster is busiest.
 */
class SecretReservationStore(
    client: KubernetesClient,
    namespace: String,
    prefix: String = SecretReservationStore.DefaultPrefix,
    maxAttempts: Int = 8,
    pollMillis: Long = 1000L)
  extends ReservationStore with Logging {

  import SecretReservationStore._

  override def update[A](cluster: String)(
      decide: Map[String, Reservation] => (Map[String, Reservation], A)): A = {
    var attempt = 0
    while (attempt < maxAttempts) {
      attempt += 1
      val existing = Option(secrets.withName(nameFor(cluster)).get())
      val held = existing.map(readFrom).getOrElse(Map.empty)
      val (next, answer) = decide(held)

      if (next == held) return answer

      try {
        if (write(cluster, existing, next)) return answer
        // write() returning false means the delete branch found the ledger
        // had moved since `existing` was read - the same "somebody else wrote
        // first" situation a 409 signals below, so it is retried the same way.
        debug(s"Reservation ledger for $cluster changed under attempt $attempt, retrying")
        backOff(attempt)
      } catch {
        case e: KubernetesClientException if e.getCode == 409 =>
          // Somebody else wrote first. The decision was made against what is now
          // a stale view, so it has to be made again - not retried with the same
          // answer, which is why `decide` is a function rather than a value.
          debug(s"Reservation ledger for $cluster changed under attempt $attempt, retrying")
          backOff(attempt)
        case NonFatal(e) =>
          throw new IllegalStateException(s"Could not record reservations for $cluster", e)
      }
    }
    throw new IllegalStateException(
      s"Gave up recording reservations for $cluster after $maxAttempts attempts")
  }

  override def read(cluster: String): Map[String, Reservation] =
    try {
      Option(secrets.withName(nameFor(cluster)).get()).map(readFrom).getOrElse(Map.empty)
    } catch {
      case NonFatal(e) =>
        warn(s"Could not read reservations for $cluster", e)
        Map.empty
    }

  override def clusters: Seq[String] =
    try {
      secrets.withLabel(LedgerLabel, LedgerLabelValue).list().getItems.asScala.toSeq
        .flatMap(s => Option(s.getMetadata.getAnnotations).flatMap(a => Option(a.get(ClusterKey))))
    } catch {
      case NonFatal(e) =>
        warn("Could not list reservation ledgers", e)
        Seq.empty
    }

  /**
   * Sleeps rather than waits.
   *
   * A release on another replica cannot wake this one - there is no signal to
   * wait on - so a waiter polls. The interval decides how quickly freed capacity
   * is noticed, and is bounded by the caller's own deadline so a short hold is
   * not rounded up to a long one.
   */
  override def awaitRelease(timeoutMillis: Long): Unit =
    if (timeoutMillis > 0) Thread.sleep(math.min(timeoutMillis, pollMillis))

  override def signalRelease(): Unit = ()

  /**
   * Pauses briefly before retrying a lost write.
   *
   * Without it a burst of concurrent admissions retries as fast as the network
   * allows and turns contention into a load spike on the api server. Jittered
   * so that replicas which collided once do not collide again in step.
   */
  private def backOff(attempt: Int): Unit = {
    val ceiling = math.min(attempt * 10, 100)
    Thread.sleep(scala.util.Random.nextInt(ceiling) + 1L)
  }

  private def secrets = client.secrets().inNamespace(namespace)

  /** @return false when the delete branch found the ledger had moved and nothing was written. */
  private def write(
      cluster: String,
      existing: Option[Secret],
      reservations: Map[String, Reservation]): Boolean = {
    val name = nameFor(cluster)
    if (reservations.isEmpty) {
      return existing match {
        case None => true
        case Some(e) =>
          // Plain delete-by-name carries no resourceVersion precondition the
          // way create/update below do, so the version is checked by hand
          // immediately before deleting. Not a true compare-and-swap - another
          // write can still land in the gap - but it closes the window a
          // blind delete left open: a concurrent admission whose write has
          // already landed is caught here and retried against the new state,
          // instead of being silently deleted along with the ledger it just
          // wrote into.
          val current = Option(secrets.withName(name).get())
          val sameVersion = current.exists(
            _.getMetadata.getResourceVersion == e.getMetadata.getResourceVersion)
          if (sameVersion) {
            secrets.withName(name).delete()
            true
          } else {
            false
          }
      }
    }
    val secret = new SecretBuilder()
      .withNewMetadata()
      .withName(name)
      .withNamespace(namespace)
      .withLabels(Map(LedgerLabel -> LedgerLabelValue).asJava)
      .withAnnotations(Map(ClusterKey -> cluster).asJava)
      // Carrying the resourceVersion is the whole mechanism: without it the
      // write is unconditional and silently overwrites a concurrent admission.
      .withResourceVersion(existing.map(_.getMetadata.getResourceVersion).orNull)
      .endMetadata()
      .withType(SecretType)
      // stringData, so what goes over the wire is the ledger as written rather
      // than a base64 blob nobody can read in an audit log. The api server
      // encodes it into `data` on the way in.
      .withStringData(Map(DataKey -> toJson(reservations)).asJava)
      .build()

    if (existing.isDefined) {
      secrets.resource(secret).update()
    } else {
      secrets.resource(secret).create()
    }
    true
  }

  private def readFrom(secret: Secret): Map[String, Reservation] =
    Option(secret.getData).flatMap(d => Option(d.get(DataKey))) match {
      case None => Map.empty
      case Some(encoded) =>
        try {
          fromJson(new String(Base64.getDecoder.decode(encoded), UTF_8))
        } catch {
          case NonFatal(e) =>
            // An unreadable ledger is treated as empty rather than fatal: the
            // alternative is a cluster that can never be admitted to again, and
            // the reconciler restores the truth from the cluster anyway.
            warn(s"Reservation ledger ${secret.getMetadata.getName} is unreadable, ignoring it", e)
            Map.empty
        }
    }

  /**
   * Secret names must be DNS subdomains; cluster names are namespace/name.
   *
   * The readable part is kept as a prefix so an operator can tell at a glance
   * which ledger belongs to which cluster, and a hash of the full name is
   * appended so two clusters cannot collide onto one ledger after sanitising.
   */
  private[capacity] def nameFor(cluster: String): String = {
    val readable = cluster.toLowerCase(Locale.ROOT)
      .map(c => if (c.isLetterOrDigit || c == '-') c else '-')
      .dropWhile(!_.isLetterOrDigit)
      .take(40)
      .mkString
    s"$prefix-$readable-${digest(cluster)}".replaceAll("-+", "-")
  }

  private def digest(value: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(UTF_8))
      .take(5)
      .map(b => f"${b & 0xFF}%02x")
      .mkString
}

object SecretReservationStore {

  val ENABLED_KEY = "kyuubi.gateway.admission.shared"
  val NAMESPACE_KEY = "kyuubi.gateway.admission.sharedNamespace"
  val POLL_KEY = "kyuubi.gateway.admission.sharedPollInterval"

  val DefaultPrefix = "kyuubi-gw"

  /** Its own type, so a ledger is never mistaken for a credential. */
  val SecretType = "kyuubi.gateway/reservations"

  val LedgerLabel = "kyuubi.gateway/ledger"
  val LedgerLabelValue = "reservations"
  val ClusterKey = "kyuubi.gateway/cluster"
  val DataKey = "reservations.json"

  private val mapper = new ObjectMapper()

  /**
   * Written field by field rather than by reflecting over a case class.
   *
   * A plain ObjectMapper finds no bean properties on a Scala class and writes
   * `{}` for it - silently, producing a ledger that round-trips as empty and an
   * accountant that admits everything. Building the nodes by hand keeps the
   * format visible and independent of which Jackson modules happen to be on the
   * classpath.
   */
  private[capacity] def toJson(reservations: Map[String, Reservation]): String = {
    val array = mapper.createArrayNode()
    reservations.values.toSeq.sortBy(_.queryId).foreach { r =>
      val row = array.addObject()
      row.put("id", r.queryId)
      row.put("bytes", r.memoryBytes)
      row.put("at", r.admittedAtMillis)
      // Omitted when false, so a ledger written by an older gateway version
      // (with no such field) still reads back as an ordinary reservation.
      if (r.synthetic) row.put("synthetic", true)
    }
    mapper.writeValueAsString(array)
  }

  private[capacity] def fromJson(json: String): Map[String, Reservation] = {
    val root = mapper.readTree(json)
    if (!root.isArray) return Map.empty
    root.elements().asScala.map { node =>
      val id = node.path("id").asText("")
      id -> Reservation(
        id,
        node.path("bytes").asLong(),
        node.path("at").asLong(),
        node.path("synthetic").asBoolean(false))
    }.filter(_._1.nonEmpty).toMap
  }
}
