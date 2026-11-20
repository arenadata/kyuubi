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

import org.apache.arrow.flight.sql.FlightSqlColumnMetadata

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId

class KyuubiFlightColumnMetadataSuite extends KyuubiFunSuite {

  private def meta(
      name: String,
      precision: Option[Int] = None,
      scale: Option[Int] = None,
      length: Option[Int] = None,
      result: Boolean = true): FlightSqlColumnMetadata =
    new FlightSqlColumnMetadata(KyuubiFlightColumnMetadata(
      name,
      precision,
      scale,
      length,
      result,
      if (result) None else Some("s"),
      if (result) None else Some("t")))

  test("base type names") {
    assert(KyuubiFlightColumnMetadata.baseName("decimal(10,2)") === "DECIMAL")
    assert(KyuubiFlightColumnMetadata.baseName("array<int>") === "ARRAY")
    assert(KyuubiFlightColumnMetadata.baseName("VARCHAR(5)") === "VARCHAR")
    assert(KyuubiFlightColumnMetadata.baseName(null: String) === "STRING")
    assert(KyuubiFlightColumnMetadata.baseName(TTypeId.TIMESTAMP_TYPE) === "TIMESTAMP")
  }

  test("type name, precision, scale and flags") {
    val dec = meta("DECIMAL", Some(10), Some(2))
    assert(dec.getTypeName === "DECIMAL")
    assert(dec.getPrecision === 10)
    assert(dec.getScale === 2)
    assert(dec.isCaseSensitive === false)
    assert(dec.isSearchable === true)
    assert(dec.isReadOnly === true)
    assert(dec.isAutoIncrement === false)
    assert(meta("VARCHAR", length = Some(5)).getPrecision === 5)
    assert(meta("CHAR", length = Some(3)).getPrecision === 3)
    val str = meta("STRING", result = false)
    assert(str.getPrecision === Int.MaxValue)
    assert(str.isCaseSensitive === true)
    assert(str.isReadOnly === false)
    assert(str.getSchemaName === "s")
    assert(str.getTableName === "t")
    val arr = meta("ARRAY")
    assert(arr.isSearchable === false)
    assert(arr.getPrecision === Int.MaxValue)
    assert(meta("TIMESTAMP").getScale === 6)
    // fixed-size types: no precision, the client derives it
    assert(meta("INT").getPrecision === null)
  }
}
