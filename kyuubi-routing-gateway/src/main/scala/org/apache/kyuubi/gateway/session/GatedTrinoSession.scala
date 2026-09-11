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

import scala.collection.JavaConverters._

import io.trino.client.ClientSession

import org.apache.kyuubi.engine.trino.session.TrinoSessionImpl
import org.apache.kyuubi.gateway.capacity.{AdmissionGate, AdmissionTicket, SessionAdmission}
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.gateway.sizing.TrinoQueryPlanner
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.session.SessionManager
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

/**
 * A routed Trino session that passes each statement through the admission gate.
 *
 * Admission is per statement, not per session: a session costs the cluster
 * nothing until it runs something, and its statements can differ by orders of
 * magnitude in what they need.
 */
class GatedTrinoSession(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager,
    gate: AdmissionGate,
    cluster: ClusterRef)
  extends TrinoSessionImpl(protocol, user, password, ipAddress, conf, sessionManager) {

  // `trinoContext` is assigned in open(), which runs after construction, so the
  // planner takes it by name. Nothing plans before the session is open: the
  // first statement cannot arrive earlier.
  private val admission = new SessionAdmission(
    gate,
    cluster,
    new TrinoQueryPlanner(trinoContext, sessionConf))

  override def executeStatement(
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle =
    admission.admitAndRun(statement) { ticket =>
      declare(ticket)
      super.executeStatement(statement, confOverlay, runAsync, queryTimeout)
    }

  private lazy val maxWait: Option[String] =
    sessionConf.getOption(GatedTrinoSession.REQUIRED_WORKERS_MAX_WAIT_KEY).filter(_.nonEmpty)

  /**
   * Tells the cluster what this query was admitted for.
   *
   * Two things travel: the worker count, because Trino holds a query in
   * WAITING_FOR_RESOURCES until that many are registered - which is what makes
   * a reservation made ahead of a scale-up mean anything - and the reservation
   * id as a client tag, so the coordinator's own list of running queries can be
   * matched back to what the gateway believes it is holding.
   */
  private def declare(ticket: AdmissionTicket): Unit =
    trinoContext.clientSession.updateAndGet { session =>
      val withTag = GatedTrinoSession.taggedWith(session, ticket.reservationId)
      if (ticket.workers > 0) {
        GatedTrinoSession.withRequiredWorkers(withTag, ticket.workers, maxWait)
      } else {
        withTag
      }
    }

  override def closeOperation(operationHandle: OperationHandle): Unit = {
    // Released before delegating: if closing throws, the reservation is still
    // gone. Holding capacity for an operation nobody can close again is worse
    // than releasing slightly early.
    admission.finished(operationHandle)
    super.closeOperation(operationHandle)
  }

  override def close(): Unit = {
    admission.releaseAll()
    super.close()
  }
}

object GatedTrinoSession {

  /**
   * How long a query may wait for the workers it was admitted for.
   *
   * Left to the cluster's own default when unset. It is worth setting when the
   * gateway is the reason for the wait: a query held for workers that a
   * scale-up never delivers would otherwise sit there for Trino's default of
   * five minutes with nothing to show for it.
   */
  val REQUIRED_WORKERS_MAX_WAIT_KEY = "kyuubi.gateway.admission.requiredWorkersMaxWait"

  /**
   * Returns the session with the worker requirement applied.
   *
   * Session properties rather than a statement prefix: `required_workers_count`
   * is a session property, and the client session is the only place the Trino
   * client reads them from. Copying the existing properties rather than
   * replacing them keeps whatever the cluster declaration or the client set.
   */
  /** Prefix that marks a client tag as the gateway's, not the client's own. */
  val RESERVATION_TAG_PREFIX = "kyuubi-reservation:"

  /** Reads the reservation id out of a query's client tags, if it carries one. */
  def reservationOf(tags: Iterable[String]): Option[String] =
    tags.find(_.startsWith(RESERVATION_TAG_PREFIX)).map(_.substring(RESERVATION_TAG_PREFIX.length))

  /**
   * Returns the session tagged with the reservation this statement holds.
   *
   * The gateway's own tag replaces the previous one rather than accumulating:
   * a session runs many statements, and a query carrying the tags of every
   * statement before it would match every one of those reservations.
   */
  def taggedWith(session: ClientSession, reservationId: String): ClientSession = {
    val kept = session.getClientTags.asScala.filterNot(_.startsWith(RESERVATION_TAG_PREFIX))
    val tags = (kept + (RESERVATION_TAG_PREFIX + reservationId)).asJava
    ClientSession.builder(session).clientTags(tags).build()
  }

  def withRequiredWorkers(
      session: ClientSession,
      workers: Int,
      maxWait: Option[String]): ClientSession = {
    val properties = new java.util.HashMap[String, String](session.getProperties)
    properties.put("required_workers_count", workers.toString)
    maxWait.foreach(properties.put("required_workers_max_wait_time", _))
    ClientSession.builder(session).properties(properties).build()
  }
}
