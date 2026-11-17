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

package org.apache.kyuubi.engine.jdbc.operation

import java.sql.Connection

import org.apache.kyuubi.config.KyuubiConf.ENGINE_JDBC_METADATA_OPERATION_TIMEOUT
import org.apache.kyuubi.engine.jdbc.schema.{Column, Row, Schema}
import org.apache.kyuubi.engine.jdbc.session.JdbcSessionImpl
import org.apache.kyuubi.operation.{ArrayFetchIterator, OperationState}
import org.apache.kyuubi.session.Session

/**
 * A metadata operation whose result set is assembled inside the engine.
 *
 * Used by dialects whose backend has no `information_schema` (e.g. Impala), where the
 * JDBC-shaped metadata result (`TABLE_SCHEM`, `TABLE_NAME`, ...) has to be built from
 * `SHOW` / `DESCRIBE` output instead of a single SQL query.
 *
 * `rows` gets the session's backend connection and a check to call before each backend
 * statement: it fails once the operation is cancelled or closed, or has run longer than
 * `kyuubi.engine.jdbc.metadata.operation.timeout`, so a request over many tables stops
 * between statements instead of running to the end.
 *
 * @param columns result set columns, in JDBC `DatabaseMetaData` order
 * @param rows    computes the rows with the session's backend connection
 */
class JdbcMetadataOperation(
    session: Session,
    columns: List[Column],
    rows: (Connection, () => Unit) => Seq[Row])
  extends JdbcOperation(session) {

  // known up front, so GetResultSetMetadata also works for a failed operation
  schema = Schema(columns)

  private lazy val timeoutMs: Long = conf.get(ENGINE_JDBC_METADATA_OPERATION_TIMEOUT)

  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    try {
      val connection = session.asInstanceOf[JdbcSessionImpl].sessionConnection
      val startedAt = System.currentTimeMillis()
      val result = rows(connection, () => checkRunning(startedAt))
      iter = new ArrayFetchIterator(result.toArray)
      setState(OperationState.FINISHED)
    } catch {
      onError()
    }
  }

  private def checkRunning(startedAt: Long): Unit = {
    if (isTerminalState(state)) {
      throw new MetadataOperationStopped(s"The metadata request was $state")
    }
    if (timeoutMs > 0 && System.currentTimeMillis() - startedAt > timeoutMs) {
      throw new MetadataOperationStopped(
        s"The metadata request did not finish within ${timeoutMs / 1000} s " +
          s"(${ENGINE_JDBC_METADATA_OPERATION_TIMEOUT.key}); narrow the schema or table pattern")
    }
  }

  override def cancel(): Unit = withLockRequired {
    if (!isTerminalState(state)) {
      setState(OperationState.CANCELED)
    }
  }

  override def shouldRunAsync: Boolean = false
}

/**
 * Thrown by the check of [[JdbcMetadataOperation]]; code that skips objects it cannot read
 * (e.g. a table that cannot be described) must rethrow it.
 */
class MetadataOperationStopped(message: String) extends RuntimeException(message)
