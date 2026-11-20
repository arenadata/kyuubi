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

import scala.collection.JavaConverters._

import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas
import org.apache.arrow.flight.sql.SqlInfoBuilder
import org.apache.arrow.flight.sql.impl.FlightSql.{SqlInfo, SqlNullOrdering, SqlSupportedCaseSensitivity, SqlSupportedTransaction}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.kyuubi.KYUUBI_VERSION

/**
 * Flight SQL GetSqlInfo of Kyuubi. Arrow's `SqlInfoBuilder.send` fails (NullPointerException)
 * on an info id it has no value for, so ids that are not registered here are left out of the
 * answer, as the Flight SQL spec allows, instead of failing the whole request.
 */
private[flight] object KyuubiFlightSqlInfo {

  private val builder = new SqlInfoBuilder()
    .withFlightSqlServerName("Kyuubi")
    .withFlightSqlServerVersion(KYUUBI_VERSION)
    .withFlightSqlServerArrowVersion("16.0.0")
    .withFlightSqlServerReadOnly(false)
    .withFlightSqlServerSql(true)
    .withFlightSqlServerSubstrait(false)
    .withFlightSqlServerCancel(true)
    .withFlightSqlServerTransaction(SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
    .withSqlIdentifierQuoteChar("`")
    .withSqlIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
    .withSqlQuotedIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
    .withSqlNullOrdering(SqlNullOrdering.SQL_NULLS_SORTED_AT_END)

  /** The ids registered in [[builder]] (kept in sync by KyuubiFlightSqlInfoSuite). */
  val registered: Set[Int] = Set(
    SqlInfo.FLIGHT_SQL_SERVER_NAME,
    SqlInfo.FLIGHT_SQL_SERVER_VERSION,
    SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION,
    SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY,
    SqlInfo.FLIGHT_SQL_SERVER_SQL,
    SqlInfo.FLIGHT_SQL_SERVER_SUBSTRAIT,
    SqlInfo.FLIGHT_SQL_SERVER_CANCEL,
    SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION,
    SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR,
    SqlInfo.SQL_IDENTIFIER_CASE,
    SqlInfo.SQL_QUOTED_IDENTIFIER_CASE,
    SqlInfo.SQL_NULL_ORDERING).map(_.getNumber)

  /** Answer GetSqlInfo; `requested` empty means all registered ids. */
  def send(
      requested: java.util.List[Integer],
      allocator: BufferAllocator,
      listener: ServerStreamListener): Unit = {
    val ids = Option(requested).map(_.asScala.toSeq).getOrElse(Seq.empty)
    val known = ids.filter(id => registered.contains(id.intValue()))
    if (ids.nonEmpty && known.isEmpty) {
      // nothing we can answer; an empty list would make the builder send everything
      val root = VectorSchemaRoot.create(Schemas.GET_SQL_INFO_SCHEMA, allocator)
      try {
        listener.start(root)
        listener.completed()
      } finally {
        root.close()
      }
    } else {
      builder.send(known.asJava, listener)
    }
  }
}
