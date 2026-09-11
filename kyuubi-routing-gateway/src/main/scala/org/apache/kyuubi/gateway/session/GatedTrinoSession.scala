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

import io.trino.client.ClientSession

import org.apache.kyuubi.engine.trino.session.TrinoSessionImpl
import org.apache.kyuubi.gateway.capacity.{AdmissionGate, SessionAdmission}
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
    admission.admitAndRun(statement) { workers =>
      requireWorkers(workers)
      super.executeStatement(statement, confOverlay, runAsync, queryTimeout)
    }

  private lazy val maxWait: Option[String] =
    sessionConf.getOption(GatedTrinoSession.REQUIRED_WORKERS_MAX_WAIT_KEY).filter(_.nonEmpty)

  /**
   * Tells the cluster how many workers this query was admitted for.
   *
   * Trino holds a query in WAITING_FOR_RESOURCES until this many workers are
   * registered, which is what makes the reservation mean anything: without it
   * a query admitted on the assumption of a scale-up would start immediately
   * on whichever workers happen to exist and get the resources of neither.
   */
  private def requireWorkers(workers: Int): Unit =
    if (workers > 0) {
      trinoContext.clientSession.updateAndGet(
        GatedTrinoSession.withRequiredWorkers(_, workers, maxWait))
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
