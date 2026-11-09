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

package org.apache.kyuubi.server.flight

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.google.protobuf.{Any => ProtoAny, ByteString}
import org.apache.arrow.flight.{CallStatus, FlightDescriptor, FlightEndpoint, FlightInfo, FlightRuntimeException, FlightStream, Location, PutResult, Result, SchemaResult, Ticket}
import org.apache.arrow.flight.FlightProducer.{CallContext, ServerStreamListener, StreamListener}
import org.apache.arrow.flight.sql.{CancelResult, NoOpFlightSqlProducer, SqlInfoBuilder}
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.impl.FlightSql._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.kyuubi.{KYUUBI_VERSION, Logging}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS, FRONTEND_FLIGHT_SQL_TOKEN_TTL, OPERATION_RESULT_FORMAT}
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.{FetchOrientation, OperationHandle, OperationState, OperationStatus}
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.SessionHandle
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class KyuubiFlightSqlProducer(
    backend: BackendService,
    allocator: BufferAllocator,
    location: () => Location,
    conf: KyuubiConf)
  extends NoOpFlightSqlProducer with Logging {

  private case class QueryState(
      owner: String,
      session: SessionHandle,
      operation: OperationHandle,
      schema: Schema,
      ownerEndpoint: String)

  private val queryStates = new ConcurrentHashMap[String, QueryState]()

  private val fetchPageSize = conf.get(FRONTEND_FLIGHT_SQL_FETCH_MAX_ROWS)

  private val sqlInfoBuilder = new SqlInfoBuilder()
    .withFlightSqlServerName("Kyuubi")
    .withFlightSqlServerVersion(KYUUBI_VERSION)
    .withFlightSqlServerArrowVersion("16.0.0")
    .withFlightSqlServerSql(true)
    .withFlightSqlServerSubstrait(false)
    .withFlightSqlServerCancel(true)
    .withFlightSqlServerTransaction(SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
    .withSqlIdentifierQuoteChar("`")
    .withSqlIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
    .withSqlQuotedIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
    .withSqlNullOrdering(SqlNullOrdering.SQL_NULLS_SORTED_AT_END)

  override def getFlightInfoStatement(
      command: CommandStatementQuery,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo = {
    try {
      registerQuery(executeQuery(command.getQuery, context), descriptor)
    } catch {
      case e: RuntimeException => throw e
      case NonFatal(e) => throw flightError(CallStatus.INTERNAL, e)
    }
  }

  /**
   * Run a statement for the session owner and wait for it. On success the returned state owns
   * an open session/operation (counted in FLIGHT_SQL_OPERATION_OPEN) that is released by
   * [[closeState]] after DoGet or by [[releaseQuery]].
   */
  private def executeQuery(sql: String, context: CallContext): QueryState = {
    MetricsSystem.tracing { ms =>
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_TOTAL)
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN)
    }
    val owner = ownerOf(context)
    val session =
      try openSession(owner, context)
      catch {
        case NonFatal(e) =>
          MetricsSystem.tracing { ms =>
            ms.decCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN)
            ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL)
          }
          throw e
      }
    try {
      val operation = backend.executeStatement(
        session,
        sql,
        Map.empty,
        runAsync = true,
        queryTimeout = 0)
      val status = waitForCompletion(operation, () => context.isCancelled)
      // Check the state before asking for metadata: a failed operation has no result
      // schema, and the engine error (e.g. an Impala ParseException) must reach the client.
      if (status.state != OperationState.FINISHED) {
        closeResources(session, operation)
        MetricsSystem.tracing(_.decCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN))
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL))
        throw operationError(status)
      }
      val schema = KyuubiFlightArrowUtils.schemaFromMetadata(
        backend.getResultSetMetadata(operation))
      QueryState(owner, session, operation, schema, location().getUri.toString)
    } catch {
      case NonFatal(e) =>
        try backend.closeSession(session)
        catch {
          case NonFatal(closeError) => warn("Failed to close Flight SQL session", closeError)
        }
        throw e
    }
  }

  /** Expose an executed query for DoGet under a statement ticket. */
  private def registerQuery(state: QueryState, descriptor: FlightDescriptor): FlightInfo = {
    val id = UUID.randomUUID().toString
    queryStates.put(id, state)
    new FlightInfo(
      state.schema,
      descriptor,
      java.util.Arrays.asList(new FlightEndpoint(statementTicket(id), location())),
      -1L,
      -1L)
  }

  /** Release an executed query that will never be streamed (e.g. an unused prepared result). */
  private def releaseQuery(state: QueryState): Unit = {
    closeResources(state.session, state.operation)
    MetricsSystem.tracing(_.decCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN))
  }

  override def getSchemaStatement(
      command: CommandStatementQuery,
      context: CallContext,
      descriptor: FlightDescriptor): SchemaResult = {
    val owner = ownerOf(context)
    val session = openSession(owner, context)
    var operation: OperationHandle = null
    try {
      operation = backend.executeStatement(
        session,
        command.getQuery,
        Map.empty,
        runAsync = true,
        queryTimeout = 0)
      val status = waitForCompletion(operation, () => context.isCancelled)
      if (status.state != OperationState.FINISHED) {
        throw operationError(status)
      }
      new SchemaResult(KyuubiFlightArrowUtils.schemaFromMetadata(
        backend.getResultSetMetadata(operation)))
    } catch {
      case e: RuntimeException => throw e
      case NonFatal(e) => throw flightError(CallStatus.INTERNAL, e)
    } finally {
      closeResources(session, operation)
    }
  }

  override def getStreamStatement(
      ticket: TicketStatementQuery,
      context: CallContext,
      listener: ServerStreamListener): Unit = {
    val id = ticket.getStatementHandle.toStringUtf8
    val state = Option(queryStates.get(id)).getOrElse(
      throw flightError(
        CallStatus.NOT_FOUND,
        new IllegalArgumentException("Unknown Flight SQL ticket")))
    if (state.owner != ownerOf(context)) {
      throw flightError(
        CallStatus.UNAUTHORIZED,
        new SecurityException("Flight SQL ticket owner mismatch"))
    }
    val current = location().getUri.toString
    if (state.ownerEndpoint != current) {
      throw flightError(
        CallStatus.UNAVAILABLE,
        new IllegalStateException(
          s"Flight SQL ticket is owned by ${state.ownerEndpoint}, not $current. " +
            "Retry against the owning endpoint; transparent failover is not supported."))
    }

    val iterator = new FlightResultIterator(
      backend,
      state.operation,
      state.schema,
      allocator,
      fetchPageSize,
      () => context.isCancelled || listener.isCancelled)
    listener.setOnCancelHandler(() => {
      MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_CANCELLED))
      iterator.cancel()
    })
    try {
      iterator.start(listener)
      var ok = true
      while (ok && !context.isCancelled && !listener.isCancelled) {
        ok = iterator.nextBatch()
        if (ok && iterator.currentRoot.getRowCount > 0) {
          listener.putNext()
        }
      }
      if (context.isCancelled || listener.isCancelled) {
        iterator.cancel()
      } else {
        listener.completed()
      }
    } catch {
      case NonFatal(e) =>
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL))
        error(s"Flight SQL DoGet failed for operation ${state.operation}", e)
        if (!context.isCancelled && !listener.isCancelled) listener.error(streamError(e))
    } finally {
      iterator.close()
      MetricsSystem.tracing(_.decCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN))
      closeState(id, state)
    }
  }

  override def getFlightInfoCatalogs(
      command: CommandGetCatalogs,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_CATALOGS_SCHEMA, command, descriptor)

  override def getStreamCatalogs(
      context: CallContext,
      listener: ServerStreamListener): Unit =
    streamMetadataByName(
      context,
      listener,
      Schemas.GET_CATALOGS_SCHEMA,
      KyuubiFlightMetadata.CATALOGS_SOURCES,
      session => backend.getCatalogs(session))

  override def getFlightInfoSchemas(
      command: CommandGetDbSchemas,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_SCHEMAS_SCHEMA, command, descriptor)

  override def getStreamSchemas(
      command: CommandGetDbSchemas,
      context: CallContext,
      listener: ServerStreamListener): Unit = {
    val catalog = if (command.hasCatalog) command.getCatalog else null
    val schema = if (command.hasDbSchemaFilterPattern) command.getDbSchemaFilterPattern else null
    streamMetadataByName(
      context,
      listener,
      Schemas.GET_SCHEMAS_SCHEMA,
      KyuubiFlightMetadata.SCHEMAS_SOURCES,
      s =>
        backend.getSchemas(s, Option(catalog).getOrElse(""), Option(schema).getOrElse("")))
  }

  override def getFlightInfoTables(
      command: CommandGetTables,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo = {
    val schema = if (command.getIncludeSchema) Schemas.GET_TABLES_SCHEMA
    else Schemas.GET_TABLES_SCHEMA_NO_SCHEMA
    metadataInfo(schema, command, descriptor)
  }

  override def getStreamTables(
      command: CommandGetTables,
      context: CallContext,
      listener: ServerStreamListener): Unit = {
    val catalog = if (command.hasCatalog) command.getCatalog else null
    val schema = if (command.hasDbSchemaFilterPattern) command.getDbSchemaFilterPattern else null
    val table = if (command.hasTableNameFilterPattern) command.getTableNameFilterPattern else null
    val tableSchemas: Option[SessionHandle => Map[KyuubiFlightMetadata.TableKey, Array[Byte]]] =
      if (command.getIncludeSchema) {
        Some(s =>
          KyuubiFlightMetadata.tableSchemas(collectMetadataRows(
            context,
            backend.getColumns(
              s,
              Option(catalog).getOrElse(""),
              Option(schema).getOrElse(""),
              Option(table).getOrElse(""),
              "%"))))
      } else {
        None
      }
    streamMetadataByName(
      context,
      listener,
      if (command.getIncludeSchema) Schemas.GET_TABLES_SCHEMA
      else Schemas.GET_TABLES_SCHEMA_NO_SCHEMA,
      KyuubiFlightMetadata.TABLES_SOURCES,
      s =>
        backend.getTables(
          s,
          Option(catalog).getOrElse(""),
          Option(schema).getOrElse(""),
          Option(table).getOrElse(""),
          command.getTableTypesList),
      tableSchemas)
  }

  override def getFlightInfoTableTypes(
      command: CommandGetTableTypes,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_TABLE_TYPES_SCHEMA, command, descriptor)

  override def getStreamTableTypes(
      context: CallContext,
      listener: ServerStreamListener): Unit =
    streamMetadataByName(
      context,
      listener,
      Schemas.GET_TABLE_TYPES_SCHEMA,
      KyuubiFlightMetadata.TABLE_TYPES_SOURCES,
      session => backend.getTableTypes(session))

  // Keys (DBeaver ER diagrams). Engines that cannot answer key requests (most JDBC dialects
  // throw "feature not supported") get an empty result instead of an error, so clients treat
  // the tables as having no keys.

  override def getFlightInfoPrimaryKeys(
      command: CommandGetPrimaryKeys,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_PRIMARY_KEYS_SCHEMA, command, descriptor)

  override def getStreamPrimaryKeys(
      command: CommandGetPrimaryKeys,
      context: CallContext,
      listener: ServerStreamListener): Unit = {
    val catalog = if (command.hasCatalog) command.getCatalog else null
    val schema = if (command.hasDbSchema) command.getDbSchema else null
    streamMetadataByName(
      context,
      listener,
      Schemas.GET_PRIMARY_KEYS_SCHEMA,
      KyuubiFlightMetadata.PRIMARY_KEYS_SOURCES,
      s => backend.getPrimaryKeys(s, catalog, schema, command.getTable),
      emptyOnError = true)
  }

  override def getFlightInfoImportedKeys(
      command: CommandGetImportedKeys,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_IMPORTED_KEYS_SCHEMA, command, descriptor)

  /** Foreign keys of the table: the table is the foreign side. */
  override def getStreamImportedKeys(
      command: CommandGetImportedKeys,
      context: CallContext,
      listener: ServerStreamListener): Unit =
    streamForeignKeys(
      context,
      listener,
      Schemas.GET_IMPORTED_KEYS_SCHEMA,
      (null, null, null),
      (
        if (command.hasCatalog) command.getCatalog else null,
        if (command.hasDbSchema) command.getDbSchema else null,
        command.getTable))

  override def getFlightInfoExportedKeys(
      command: CommandGetExportedKeys,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_EXPORTED_KEYS_SCHEMA, command, descriptor)

  /** Foreign keys referencing the table: the table is the primary side. */
  override def getStreamExportedKeys(
      command: CommandGetExportedKeys,
      context: CallContext,
      listener: ServerStreamListener): Unit =
    streamForeignKeys(
      context,
      listener,
      Schemas.GET_EXPORTED_KEYS_SCHEMA,
      (
        if (command.hasCatalog) command.getCatalog else null,
        if (command.hasDbSchema) command.getDbSchema else null,
        command.getTable),
      (null, null, null))

  override def getFlightInfoCrossReference(
      command: CommandGetCrossReference,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_CROSS_REFERENCE_SCHEMA, command, descriptor)

  override def getStreamCrossReference(
      command: CommandGetCrossReference,
      context: CallContext,
      listener: ServerStreamListener): Unit =
    streamForeignKeys(
      context,
      listener,
      Schemas.GET_CROSS_REFERENCE_SCHEMA,
      (
        if (command.hasPkCatalog) command.getPkCatalog else null,
        if (command.hasPkDbSchema) command.getPkDbSchema else null,
        command.getPkTable),
      (
        if (command.hasFkCatalog) command.getFkCatalog else null,
        if (command.hasFkDbSchema) command.getFkDbSchema else null,
        command.getFkTable))

  private def streamForeignKeys(
      context: CallContext,
      listener: ServerStreamListener,
      schema: Schema,
      primary: (String, String, String),
      foreign: (String, String, String)): Unit =
    streamMetadataByName(
      context,
      listener,
      schema,
      KyuubiFlightMetadata.FOREIGN_KEYS_SOURCES,
      s =>
        backend.getCrossReference(
          s,
          primary._1,
          primary._2,
          primary._3,
          foreign._1,
          foreign._2,
          foreign._3),
      emptyOnError = true)

  override def getFlightInfoTypeInfo(
      command: CommandGetXdbcTypeInfo,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_TYPE_INFO_SCHEMA, command, descriptor)

  override def getFlightInfoSqlInfo(
      command: CommandGetSqlInfo,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo =
    metadataInfo(Schemas.GET_SQL_INFO_SCHEMA, command, descriptor)

  override def getStreamSqlInfo(
      command: CommandGetSqlInfo,
      context: CallContext,
      listener: ServerStreamListener): Unit =
    sqlInfoBuilder.send(command.getInfoList, listener)

  override def getStreamTypeInfo(
      command: CommandGetXdbcTypeInfo,
      context: CallContext,
      listener: ServerStreamListener): Unit =
    streamMetadata(
      context,
      listener,
      Schemas.GET_TYPE_INFO_SCHEMA,
      session =>
        backend.getTypeInfo(session))

  // ------------------------------------------------------------------ prepared statements
  //
  // Parameterless prepared statements, needed by the Arrow Flight SQL JDBC driver (DBeaver),
  // which runs every statement through CreatePreparedStatement and picks the update path when
  // the returned dataset schema is empty. Queries run at prepare time to get their schema;
  // that first result is cached and served by the first execution. Other statements run only
  // when executed, so preparing them has no side effects.

  private class PreparedState(
      val owner: String,
      val sql: String,
      val isQuery: Boolean,
      val schema: Schema,
      initialResult: Option[QueryState]) {
    private var cached: Option[QueryState] = initialResult
    @volatile var lastAccess: Long = System.currentTimeMillis()

    def touch(): Unit = lastAccess = System.currentTimeMillis()

    /** The result computed at prepare time; returned at most once. */
    def takeCached(): Option[QueryState] = synchronized {
      val result = cached
      cached = None
      result
    }
  }

  private val preparedStates = new ConcurrentHashMap[String, PreparedState]()

  private val preparedStatementIdleTimeoutMs = conf.get(FRONTEND_FLIGHT_SQL_TOKEN_TTL)

  private val emptySchema = new Schema(java.util.Collections.emptyList())

  override def createPreparedStatement(
      request: ActionCreatePreparedStatementRequest,
      context: CallContext,
      listener: StreamListener[Result]): Unit = {
    try {
      expireIdlePreparedStatements()
      val sql = request.getQuery
      val owner = ownerOf(context)
      val isQuery = KyuubiFlightSqlStatements.isQuery(sql)
      val result = if (isQuery) Some(executeQuery(sql, context)) else None
      val schema = result.map(_.schema).getOrElse(emptySchema)
      val handle = UUID.randomUUID().toString
      preparedStates.put(handle, new PreparedState(owner, sql, isQuery, schema, result))
      debug(s"Flight SQL prepared statement $handle for $owner (query=$isQuery)")
      val response = ActionCreatePreparedStatementResult.newBuilder()
        .setPreparedStatementHandle(ByteString.copyFrom(handle, StandardCharsets.UTF_8))
        .setDatasetSchema(ByteString.copyFrom(schema.serializeAsMessage()))
        .setParameterSchema(ByteString.copyFrom(emptySchema.serializeAsMessage()))
        .build()
      listener.onNext(new Result(ProtoAny.pack(response).toByteArray))
      listener.onCompleted()
    } catch {
      case NonFatal(e) => listener.onError(streamError(e))
    }
  }

  override def closePreparedStatement(
      request: ActionClosePreparedStatementRequest,
      context: CallContext,
      listener: StreamListener[Result]): Unit = {
    try {
      val handle = request.getPreparedStatementHandle.toStringUtf8
      Option(preparedStates.get(handle)).foreach { state =>
        checkOwner(state, context)
        if (preparedStates.remove(handle, state)) {
          state.takeCached().foreach(releaseQuery)
        }
      }
      listener.onCompleted()
    } catch {
      case NonFatal(e) => listener.onError(streamError(e))
    }
  }

  override def getFlightInfoPreparedStatement(
      command: CommandPreparedStatementQuery,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo = {
    try {
      val state = preparedState(command.getPreparedStatementHandle, context)
      val result = state.takeCached().getOrElse(executeQuery(state.sql, context))
      registerQuery(result, descriptor)
    } catch {
      case e: RuntimeException => throw e
      case NonFatal(e) => throw flightError(CallStatus.INTERNAL, e)
    }
  }

  override def getSchemaPreparedStatement(
      command: CommandPreparedStatementQuery,
      context: CallContext,
      descriptor: FlightDescriptor): SchemaResult =
    new SchemaResult(preparedState(command.getPreparedStatementHandle, context).schema)

  override def acceptPutPreparedStatementUpdate(
      command: CommandPreparedStatementUpdate,
      context: CallContext,
      flightStream: FlightStream,
      ackStream: StreamListener[PutResult]): Runnable = () => {
    try {
      rejectParameters(flightStream)
      val state = preparedState(command.getPreparedStatementHandle, context)
      // a query sent through the update path: its prepare-time result is not needed
      state.takeCached().foreach(releaseQuery)
      sendUpdateCount(ackStream, executeUpdate(state.sql, context))
    } catch {
      case NonFatal(e) => ackStream.onError(streamError(e))
    }
  }

  override def acceptPutStatement(
      command: CommandStatementUpdate,
      context: CallContext,
      flightStream: FlightStream,
      ackStream: StreamListener[PutResult]): Runnable = () => {
    try {
      // CommandStatementUpdate carries no parameters, and the Arrow Java client
      // (FlightSqlClient.executeUpdate) waits for the result without closing its side of the
      // stream, so draining it here would block both ends
      sendUpdateCount(ackStream, executeUpdate(command.getQuery, context))
    } catch {
      case NonFatal(e) => ackStream.onError(streamError(e))
    }
  }

  override def acceptPutPreparedStatementQuery(
      command: CommandPreparedStatementQuery,
      context: CallContext,
      flightStream: FlightStream,
      ackStream: StreamListener[PutResult]): Runnable = () => {
    try {
      // binding an empty parameter set is a no-op; actual values are not supported
      rejectParameters(flightStream)
      preparedState(command.getPreparedStatementHandle, context)
      ackStream.onCompleted()
    } catch {
      case NonFatal(e) => ackStream.onError(streamError(e))
    }
  }

  private def preparedState(handle: ByteString, context: CallContext): PreparedState = {
    val state = Option(preparedStates.get(handle.toStringUtf8)).getOrElse(
      throw flightError(
        CallStatus.NOT_FOUND,
        new IllegalArgumentException("Unknown or expired Flight SQL prepared statement")))
    checkOwner(state, context)
    state.touch()
    state
  }

  private def checkOwner(state: PreparedState, context: CallContext): Unit = {
    if (state.owner != ownerOf(context)) {
      throw flightError(
        CallStatus.UNAUTHORIZED,
        new SecurityException("Flight SQL prepared statement owner mismatch"))
    }
  }

  private def rejectParameters(flightStream: FlightStream): Unit = {
    while (flightStream.next()) {
      if (flightStream.getRoot.getRowCount > 0) {
        throw flightError(
          CallStatus.INVALID_ARGUMENT,
          new UnsupportedOperationException(
            "Flight SQL parameter binding is not supported by Kyuubi"))
      }
    }
  }

  /**
   * Run a statement for its side effects. Returns the affected row count when the engine
   * reports one (a single numeric value, e.g. the JDBC engine's update count), else -1.
   */
  private def executeUpdate(sql: String, context: CallContext): Long = {
    MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_TOTAL))
    val session = openSession(ownerOf(context), context)
    try {
      val operation = backend.executeStatement(
        session,
        sql,
        Map.empty,
        runAsync = true,
        queryTimeout = 0)
      val rows = collectMetadataRows(context, operation)
      rows match {
        case Seq(row) if row.size == 1 =>
          row.values.head match {
            case n: Number => n.longValue()
            case _ => -1L
          }
        case _ => -1L
      }
    } catch {
      case NonFatal(e) =>
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL))
        throw e
    } finally {
      closeResources(session, null)
    }
  }

  private def sendUpdateCount(ackStream: StreamListener[PutResult], count: Long): Unit = {
    val bytes = DoPutUpdateResult.newBuilder().setRecordCount(count).build().toByteArray
    val buffer = allocator.buffer(bytes.length)
    try {
      buffer.writeBytes(bytes)
      ackStream.onNext(PutResult.metadata(buffer))
    } finally {
      buffer.close()
    }
    ackStream.onCompleted()
  }

  /** Drop prepared statements idle longer than the Flight bearer token TTL. */
  private def expireIdlePreparedStatements(): Unit = {
    val cutoff = System.currentTimeMillis() - preparedStatementIdleTimeoutMs
    preparedStates.entrySet().asScala.foreach { entry =>
      val state = entry.getValue
      if (state.lastAccess < cutoff && preparedStates.remove(entry.getKey, state)) {
        info(s"Expired idle Flight SQL prepared statement ${entry.getKey} of ${state.owner}")
        state.takeCached().foreach(releaseQuery)
      }
    }
  }

  private[flight] def preparedStatementCount: Int = preparedStates.size()

  override def cancelQuery(
      info: FlightInfo,
      context: CallContext,
      listener: StreamListener[CancelResult]): Unit = {
    try {
      val endpoint = info.getEndpoints.get(0)
      val any = ProtoAny.parseFrom(endpoint.getTicket.getBytes)
      val statementTicket = TicketStatementQuery.parseFrom(any.getValue)
      val id = statementTicket.getStatementHandle.toStringUtf8
      Option(queryStates.get(id)) match {
        case Some(state) if state.owner == ownerOf(context) =>
          cancelState(state)
          MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_CANCELLED))
          listener.onNext(CancelResult.CANCELLED)
        case Some(_) =>
          listener.onNext(CancelResult.NOT_CANCELLABLE)
        case None =>
          listener.onNext(CancelResult.NOT_CANCELLABLE)
      }
      listener.onCompleted()
    } catch {
      case NonFatal(e) => listener.onError(flightError(CallStatus.INVALID_ARGUMENT, e))
    }
  }

  override def close(): Unit = {
    queryStates.entrySet().asScala.foreach(entry => closeState(entry.getKey, entry.getValue))
    queryStates.clear()
    preparedStates.values().asScala.foreach(_.takeCached().foreach(releaseQuery))
    preparedStates.clear()
  }

  private def metadataInfo(
      schema: Schema,
      command: com.google.protobuf.Message,
      descriptor: FlightDescriptor): FlightInfo = {
    val ticket = new Ticket(ProtoAny.pack(command).toByteArray)
    new FlightInfo(
      schema,
      descriptor,
      java.util.Arrays.asList(new FlightEndpoint(ticket, location())),
      -1L,
      -1L)
  }

  /**
   * Run a metadata operation and return its rows as `lower-case column -> value`. Metadata
   * results are small, so they are collected instead of streamed page by page; both columnar
   * thrift and Arrow IPC pages are handled.
   */
  private def collectMetadataRows(
      context: CallContext,
      operation: OperationHandle): Seq[KyuubiFlightMetadata.MetadataRow] = {
    try {
      val status = waitForCompletion(operation, () => context.isCancelled)
      if (status.state != OperationState.FINISHED) throw operationError(status)
      val sourceSchema =
        KyuubiFlightArrowUtils.schemaFromMetadata(backend.getResultSetMetadata(operation))
      val source = VectorSchemaRoot.create(sourceSchema, allocator)
      try {
        val loader = new VectorLoader(source)
        val rows = ArrayBuffer[KyuubiFlightMetadata.MetadataRow]()
        var more = true
        while (more && !context.isCancelled) {
          val rowSet = backend.fetchResults(
            operation,
            FetchOrientation.FETCH_NEXT,
            fetchPageSize,
            fetchLog = false).getResults
          if (KyuubiFlightArrowUtils.isEmpty(rowSet)) {
            more = false
          } else {
            if (KyuubiFlightArrowUtils.isArrowRowSet(rowSet)) {
              val batch = KyuubiFlightArrowUtils.decodeBatch(rowSet, allocator)
              try loader.load(batch)
              finally batch.close()
            } else {
              KyuubiFlightArrowUtils.populateRootFromRowSet(source, rowSet)
            }
            rows ++= KyuubiFlightMetadata.readRows(source)
            more = source.getRowCount > 0
          }
        }
        rows.toSeq
      } finally {
        source.close()
      }
    } finally {
      try backend.closeOperation(operation)
      catch {
        case NonFatal(e) => warn(s"Failed to close Flight SQL metadata operation $operation", e)
      }
    }
  }

  /**
   * Metadata stream that maps engine (JDBC/HiveServer2-shaped) columns onto the fixed Flight
   * SQL schema by name instead of position, see [[KyuubiFlightMetadata]]. With `emptyOnError`
   * a failing engine operation yields an empty result instead of an error.
   */
  private def streamMetadataByName(
      context: CallContext,
      listener: ServerStreamListener,
      schema: Schema,
      sources: KyuubiFlightMetadata.Sources,
      operationFactory: SessionHandle => OperationHandle,
      tableSchemas: Option[SessionHandle => Map[KyuubiFlightMetadata.TableKey, Array[Byte]]] =
        None,
      emptyOnError: Boolean = false): Unit = {
    val owner = ownerOf(context)
    val session = openSession(owner, context)
    MetricsSystem.tracing { ms =>
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_TOTAL)
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN)
    }
    try {
      val rows =
        try collectMetadataRows(context, operationFactory(session))
        catch {
          case NonFatal(e) if emptyOnError =>
            warn(s"Flight SQL metadata request not answered by the engine, returning no rows: " +
              Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
            Seq.empty
        }
      val schemasByTable = tableSchemas.map(_(session)).getOrElse(Map.empty)
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        listener.start(root)
        KyuubiFlightMetadata.fill(root, rows, sources, schemasByTable)
        if (rows.nonEmpty && !context.isCancelled && !listener.isCancelled) {
          listener.putNext()
        }
        if (!context.isCancelled && !listener.isCancelled) listener.completed()
      } finally {
        root.close()
      }
    } catch {
      case NonFatal(e) =>
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL))
        error("Flight SQL metadata DoGet failed", e)
        if (!context.isCancelled && !listener.isCancelled) listener.error(streamError(e))
    } finally {
      MetricsSystem.tracing(_.decCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN))
      closeResources(session, null)
    }
  }

  private def streamMetadata(
      context: CallContext,
      listener: ServerStreamListener,
      schema: Schema,
      operationFactory: SessionHandle => OperationHandle): Unit = {
    val owner = ownerOf(context)
    val session = openSession(owner, context)
    var operation: OperationHandle = null
    var iterator: FlightResultIterator = null
    MetricsSystem.tracing { ms =>
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_TOTAL)
      ms.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN)
    }
    try {
      operation = operationFactory(session)
      val status = waitForCompletion(operation, () => context.isCancelled || listener.isCancelled)
      if (status.state != OperationState.FINISHED) throw operationError(status)
      iterator = new FlightResultIterator(
        backend,
        operation,
        schema,
        allocator,
        fetchPageSize,
        () => context.isCancelled || listener.isCancelled)
      listener.setOnCancelHandler(() => {
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_CANCELLED))
        iterator.cancel()
      })
      iterator.start(listener)
      var ok = true
      while (ok && !context.isCancelled && !listener.isCancelled) {
        ok = iterator.nextBatch()
        if (ok && iterator.currentRoot.getRowCount > 0) {
          listener.putNext()
        }
      }
      if (context.isCancelled || listener.isCancelled) {
        iterator.cancel()
      } else {
        listener.completed()
      }
    } catch {
      case NonFatal(e) =>
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_OPERATION_FAIL))
        error(s"Flight SQL metadata DoGet failed for operation $operation", e)
        if (!context.isCancelled && !listener.isCancelled) listener.error(streamError(e))
    } finally {
      if (iterator != null) iterator.close()
      MetricsSystem.tracing(_.decCount(MetricsConstants.FLIGHT_SQL_OPERATION_OPEN))
      closeResources(session, operation)
    }
  }

  /** `kyuubi.*` session configuration the client sent as call headers. */
  private def clientSessionConf(context: CallContext): Map[String, String] =
    Option(context.getMiddleware(KyuubiFlightSessionConfMiddleware.KEY))
      .map(_.sessionConf)
      .getOrElse(Map.empty)

  private def openSession(owner: String, context: CallContext): SessionHandle = {
    val clientConf = clientSessionConf(context)
    if (clientConf.nonEmpty) {
      info(s"Flight SQL session for $owner with client conf keys: " +
        clientConf.keys.toSeq.sorted.mkString(", "))
    }
    backend.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
      owner,
      "",
      "",
      // Like Thrift OpenSession: client settings first, server-owned keys win.
      clientConf ++ Map(
        KyuubiReservedKeys.KYUUBI_CLIENT_IP_KEY -> "",
        KyuubiReservedKeys.KYUUBI_SESSION_REAL_USER_KEY -> owner,
        KyuubiReservedKeys.KYUUBI_SESSION_CONNECTION_URL_KEY -> location().toString,
        OPERATION_RESULT_FORMAT.key -> "arrow",
        KyuubiConf.FRONTEND_PROTOCOLS.key ->
          KyuubiConf.FrontendProtocols.FLIGHT_SQL.toString))
  }

  private def waitForCompletion(
      operation: OperationHandle,
      isCancelled: () => Boolean): OperationStatus = {
    var status = backend.getOperationStatus(operation, Some(1000L))
    while (!OperationState.isTerminal(status.state) && !isCancelled()) {
      status = backend.getOperationStatus(operation, Some(1000L))
    }
    status
  }

  private def statementTicket(id: String): Ticket = {
    val statementTicket = TicketStatementQuery.newBuilder
      .setStatementHandle(ByteString.copyFrom(id.getBytes(StandardCharsets.UTF_8)))
      .build()
    new Ticket(ProtoAny.pack(statementTicket).toByteArray)
  }

  private def ownerOf(context: CallContext): String =
    Option(context.peerIdentity()).filter(_.nonEmpty).getOrElse("anonymous")

  private def closeState(id: String, state: QueryState): Unit = {
    if (queryStates.remove(id, state)) {
      closeResources(state.session, state.operation)
    }
  }

  private def closeResources(session: SessionHandle, operation: OperationHandle): Unit = {
    if (operation != null) {
      try backend.closeOperation(operation)
      catch {
        case NonFatal(e) => warn(s"Failed to close Flight SQL operation $operation", e)
      }
    }
    if (session != null) {
      try backend.closeSession(session)
      catch {
        case NonFatal(e) => warn(s"Failed to close Flight SQL session $session", e)
      }
    }
  }

  private def cancelState(state: QueryState): Unit = {
    try backend.cancelOperation(state.operation)
    catch {
      case NonFatal(e) => warn(s"Failed to cancel Flight SQL operation ${state.operation}", e)
    }
  }

  private def operationError(status: OperationStatus): RuntimeException =
    flightError(
      CallStatus.INTERNAL,
      status.exception.getOrElse(
        new IllegalStateException(s"Flight SQL operation ended in ${status.state}")))

  /**
   * Flight maps non-Flight exceptions to a generic "error servicing your request";
   * wrap them so the client sees the actual cause.
   */
  private def streamError(e: Throwable): Throwable = e match {
    case flight: FlightRuntimeException => flight
    case other => flightError(CallStatus.INTERNAL, other)
  }

  private def flightError(status: CallStatus, cause: Throwable): RuntimeException =
    status.withDescription(Option(cause.getMessage).getOrElse(cause.getClass.getSimpleName))
      .withCause(cause)
      .toRuntimeException
}
