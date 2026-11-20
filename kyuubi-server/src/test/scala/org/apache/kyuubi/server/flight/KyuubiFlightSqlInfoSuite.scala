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
import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{UInt4Vector, VectorSchemaRoot}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, mock}

import org.apache.kyuubi.{FlightSqlTestHelper, KyuubiFunSuite}

class KyuubiFlightSqlInfoSuite extends KyuubiFunSuite {

  /** The info ids of a GetSqlInfo answer, and whether it completed. */
  private def answer(requested: Seq[Int]): (Seq[Int], Boolean) = {
    FlightSqlTestHelper.ensureArrowUnsafeAllocator()
    val allocator = new RootAllocator()
    val ids = ArrayBuffer[Int]()
    var root: VectorSchemaRoot = null
    var completed = false
    val listener = mock(classOf[ServerStreamListener])
    doAnswer { invocation =>
      root = invocation.getArgument[VectorSchemaRoot](0)
      null
    }.when(listener).start(any[VectorSchemaRoot]())
    doAnswer { _ =>
      val names = root.getVector("info_name").asInstanceOf[UInt4Vector]
      ids ++= (0 until root.getRowCount).map(i => names.get(i))
      null
    }.when(listener).putNext()
    doAnswer { _ =>
      completed = true
      null
    }.when(listener).completed()
    try {
      KyuubiFlightSqlInfo.send(requested.map(Int.box).asJava, allocator, listener)
      (ids.toSeq, completed)
    } finally {
      allocator.close()
    }
  }

  test("all registered info is sent when nothing is requested, and the id list matches") {
    // the registered set is what the builder holds: keep both in sync
    assert(answer(Seq.empty)._1.toSet === KyuubiFlightSqlInfo.registered)
  }

  test("unregistered info ids are left out instead of failing the request") {
    val readOnly = SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY.getNumber
    val name = SqlInfo.FLIGHT_SQL_SERVER_NAME.getNumber
    val unknown = SqlInfo.SQL_DDL_CATALOG.getNumber
    assert(answer(Seq(name, unknown, readOnly))._1 === Seq(name, readOnly))
    // only unknown ids: an empty answer, not everything
    val (ids, completed) = answer(Seq(unknown, 999999))
    assert(ids.isEmpty)
    assert(completed)
  }
}
