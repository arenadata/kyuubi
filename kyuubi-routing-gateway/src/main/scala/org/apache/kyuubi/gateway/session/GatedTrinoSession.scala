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

import org.apache.kyuubi.engine.trino.session.TrinoSessionImpl
import org.apache.kyuubi.gateway.capacity.SessionAdmission
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
    admission: SessionAdmission)
  extends TrinoSessionImpl(protocol, user, password, ipAddress, conf, sessionManager) {

  override def executeStatement(
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle =
    admission.admitAndRun(statement) { () =>
      super.executeStatement(statement, confOverlay, runAsync, queryTimeout)
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
