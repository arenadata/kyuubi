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
package org.apache.kyuubi.engine.jdbc.schema

import java.lang.{Byte => JByte, Short => JShort}
import java.nio.charset.StandardCharsets
import java.sql.Types

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.jdbc.impala.ImpalaTRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TColumn
import org.apache.kyuubi.shaded.thrift.protocol.TBinaryProtocol
import org.apache.kyuubi.shaded.thrift.transport.TMemoryBuffer

class DefaultJdbcTRowSetGeneratorSuite extends KyuubiFunSuite {

  private val generator = new ImpalaTRowSetGenerator

  private def column(sqlType: Int): Column = Column("c", "t", sqlType, 0, 0, "c", 0)

  /** Thrift serialization is where unchecked casts used to fail (Byte -> Short). */
  private def serialize(col: TColumn): Unit = {
    col.write(new TBinaryProtocol(new TMemoryBuffer(64)))
  }

  test("TINYINT values returned as Byte are widened into the i16 column") {
    // Hive/Impala drivers return TINYINT as java.lang.Byte
    val rows: Seq[Seq[_]] =
      Seq(Seq(JByte.valueOf(1.toByte)), Seq(null), Seq(JByte.valueOf(-3.toByte)))
    val col = generator.toTColumn(rows, 0, Types.TINYINT)
    assert(col.isSetI16Val)
    val values = col.getI16Val.getValues.asScala
    values.foreach(v => assert(v.getClass === classOf[JShort]))
    assert(values.map(_.toInt) === Seq(1, 0, -3))
    serialize(col)

    val value = generator.toTColumnValue(rows.head, 0, Seq(column(Types.TINYINT)))
    assert(value.getI16Val.getValue === 1.toShort)
  }

  test("SMALLINT values returned as Integer are narrowed into the i16 column") {
    // JDBC spec maps getObject(SMALLINT) to Integer
    val rows: Seq[Seq[_]] = Seq(Seq(Integer.valueOf(300)))
    val col = generator.toTColumn(rows, 0, Types.SMALLINT)
    assert(col.getI16Val.getValues.get(0) === 300.toShort)
    serialize(col)

    val value = generator.toTColumnValue(rows.head, 0, Seq(column(Types.SMALLINT)))
    assert(value.getI16Val.getValue === 300.toShort)
  }

  test("BINARY values returned as byte[] are rendered as text, not Object.toString") {
    val bytes = "ab".getBytes(StandardCharsets.UTF_8)
    val rows: Seq[Seq[_]] = Seq(Seq(bytes))
    Seq(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY).foreach { sqlType =>
      val col = generator.toTColumn(rows, 0, sqlType)
      assert(col.getStringVal.getValues.get(0) === "ab")
      val value = generator.toTColumnValue(rows.head, 0, Seq(column(sqlType)))
      assert(value.getStringVal.getValue === "ab")
    }
  }
}
