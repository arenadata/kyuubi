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

package org.apache.kyuubi

import java.io.{File, FileInputStream}
import java.util.function.Consumer

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.arrow.flight.{
  CallHeaders,
  CallOption,
  CallStatus,
  FlightClient,
  FlightClientMiddleware,
  FlightInfo,
  FlightRuntimeException,
  FlightStatusCode,
  Location,
  Ticket}
import org.apache.arrow.flight.auth2.{
  Auth2Constants,
  ClientBearerHeaderHandler,
  ClientIncomingAuthHeaderMiddleware}
import org.apache.arrow.flight.grpc.CredentialCallOption
import org.apache.arrow.flight.sql.FlightSqlClient
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.FieldVector
import org.scalatest.Assertions

trait FlightSqlTestHelper extends Assertions {

  protected def flightSqlUrl: String

  protected def flightSqlHost: String = {
    val endpoint = flightSqlUrl
    endpoint.substring(0, endpoint.lastIndexOf(':'))
  }

  protected def flightSqlPort: Int = {
    val endpoint = flightSqlUrl
    endpoint.substring(endpoint.lastIndexOf(':') + 1).toInt
  }

  protected def withFlightSqlClient[T](
      trustedCert: Option[File] = None,
      verifyServer: Boolean = true,
      middleware: Seq[FlightClientMiddleware.Factory] = Nil)(
      f: (FlightClient, FlightSqlClient) => T): T =
    withFlightSqlClientAt(flightSqlUrl, trustedCert, verifyServer, middleware)(f)

  protected def withFlightSqlClientAt[T](
      endpoint: String,
      trustedCert: Option[File] = None,
      verifyServer: Boolean = true,
      middleware: Seq[FlightClientMiddleware.Factory] = Nil)(
      f: (FlightClient, FlightSqlClient) => T): T = {
    val host = endpoint.substring(0, endpoint.lastIndexOf(':'))
    val port = endpoint.substring(endpoint.lastIndexOf(':') + 1).toInt
    val allocator = new RootAllocator()
    val builder =
      if (trustedCert.isDefined) {
        FlightClient.builder(allocator, Location.forGrpcTls(host, port))
          .useTls()
          .trustedCertificates(new FileInputStream(trustedCert.get))
          .verifyServer(verifyServer)
      } else {
        FlightClient.builder(allocator, Location.forGrpcInsecure(host, port))
      }
    middleware.foreach(builder.intercept)
    val flightClient = builder.build()
    val sqlClient = new FlightSqlClient(flightClient)
    try {
      f(flightClient, sqlClient)
    } finally {
      try sqlClient.close()
      catch { case NonFatal(_) => }
      try flightClient.close()
      catch { case NonFatal(_) => }
      try allocator.close()
      catch { case NonFatal(_) => }
    }
  }

  protected def authenticateBasic(
      flightClient: FlightClient,
      user: String,
      password: String): CallOption = {
    val bearer = flightClient.authenticateBasicToken(user, password)
    assert(bearer.isPresent, "expected Flight bearer token after Basic auth")
    bearer.get()
  }

  protected def authenticateNegotiate(
      flightClient: FlightClient,
      factory: ClientIncomingAuthHeaderMiddleware.Factory,
      negotiateToken: String): CallOption = {
    val writer = new Consumer[CallHeaders] {
      override def accept(headers: CallHeaders): Unit = {
        headers.insert(
          Auth2Constants.AUTHORIZATION_HEADER,
          "Negotiate " + negotiateToken)
      }
    }
    flightClient.handshake(new CredentialCallOption(writer))
    val bearer = factory.getCredentialCallOption
    assert(bearer != null, "expected Flight bearer token after Negotiate auth")
    bearer
  }

  protected def newBearerCaptureFactory(): ClientIncomingAuthHeaderMiddleware.Factory =
    new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler)

  protected def executeAndCollect(
      sqlClient: FlightSqlClient,
      sql: String,
      options: CallOption*): Seq[Seq[AnyRef]] = {
    executeAndCollectPaged(sqlClient, sql, options: _*).rows
  }

  protected def executeAndCollectPaged(
      sqlClient: FlightSqlClient,
      sql: String,
      options: CallOption*): FlightStreamPages = {
    val info = sqlClient.execute(sql, options: _*)
    assert(info.getEndpoints.size() === 1)
    readTicketPages(sqlClient, info.getEndpoints.get(0).getTicket, options: _*)
  }

  protected def readAllRows(
      sqlClient: FlightSqlClient,
      info: FlightInfo,
      options: CallOption*): Seq[Seq[AnyRef]] = {
    assert(info.getEndpoints.size() === 1)
    val ticket = info.getEndpoints.get(0).getTicket
    readTicketRows(sqlClient, ticket, options: _*)
  }

  protected def readTicketRows(
      sqlClient: FlightSqlClient,
      ticket: Ticket,
      options: CallOption*): Seq[Seq[AnyRef]] =
    readTicketPages(sqlClient, ticket, options: _*).rows

  protected def readTicketPages(
      sqlClient: FlightSqlClient,
      ticket: Ticket,
      options: CallOption*): FlightStreamPages = {
    val rows = ArrayBuffer.empty[Seq[AnyRef]]
    val batchRowCounts = ArrayBuffer.empty[Int]
    val stream = sqlClient.getStream(ticket, options: _*)
    try {
      while (stream.next()) {
        val root = stream.getRoot
        val batchSize = root.getRowCount
        if (batchSize > 0) {
          batchRowCounts += batchSize
          val vectors = (0 until root.getFieldVectors.size()).map(root.getVector)
          (0 until batchSize).foreach { rowIdx =>
            rows += vectors.map(vectorValue(_, rowIdx))
          }
        }
      }
    } finally {
      stream.close()
    }
    FlightStreamPages(rows.toSeq, batchRowCounts.toSeq)
  }

  protected def assertFlightExactPageSizes(
      batchRowCounts: Seq[Int],
      expectedTotalRows: Int,
      pageSize: Int): Unit = {
    assert(batchRowCounts.sum === expectedTotalRows, s"batch sizes $batchRowCounts")
    assert(batchRowCounts.nonEmpty, "expected at least one Arrow batch")
    val expectedBatches = (expectedTotalRows + pageSize - 1) / pageSize
    assert(
      batchRowCounts.size === expectedBatches,
      s"expected $expectedBatches batches of pageSize=$pageSize, got $batchRowCounts")
    assert(
      batchRowCounts.init.forall(_ === pageSize),
      s"non-final batches must be exactly $pageSize rows, got $batchRowCounts")
    assert(
      batchRowCounts.last > 0 && batchRowCounts.last <= pageSize,
      s"final batch must be in 1..$pageSize, got $batchRowCounts")
  }

  protected def assertFlightBatchesBoundedByPageSize(
      batchRowCounts: Seq[Int],
      expectedTotalRows: Int,
      pageSize: Int): Unit = {
    assert(batchRowCounts.sum === expectedTotalRows, s"batch sizes $batchRowCounts")
    assert(batchRowCounts.nonEmpty, "expected at least one Arrow batch")
    assert(
      batchRowCounts.forall(c => c > 0 && c <= pageSize),
      s"each batch must be in 1..$pageSize, got $batchRowCounts")
    if (expectedTotalRows > pageSize) {
      assert(
        batchRowCounts.size > 1,
        s"expected multiple batches for $expectedTotalRows rows with pageSize=$pageSize")
    }
  }

  protected def vectorValue(vector: FieldVector, rowIdx: Int): AnyRef = {
    if (vector.isNull(rowIdx)) null
    else vector.getObject(rowIdx)
  }

  protected def assertFlightStatus(code: FlightStatusCode)(body: => Any): Unit = {
    val e = intercept[FlightRuntimeException](body)
    assert(e.status().code() === code, s"expected $code, got ${e.status()}")
  }

  protected def assertFlightStatus(status: CallStatus)(body: => Any): Unit =
    assertFlightStatus(status.code())(body)
}

case class FlightStreamPages(rows: Seq[Seq[AnyRef]], batchRowCounts: Seq[Int])

