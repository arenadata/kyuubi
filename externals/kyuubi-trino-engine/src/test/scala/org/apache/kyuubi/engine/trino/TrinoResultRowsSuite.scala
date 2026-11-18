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

package org.apache.kyuubi.engine.trino

import java.util.{Arrays, Collections, Iterator => JIterator, List => JList}

import io.trino.client.ResultRows

import org.apache.kyuubi.KyuubiFunSuite

class TrinoResultRowsSuite extends KyuubiFunSuite {
  test("no result page differs from an empty result page") {
    assert(TrinoStatement.readRows(ResultRows.NULL_ROWS).isEmpty)
    var closed = false
    val rows = new ResultRows {
      override def iterator(): JIterator[JList[Object]] = Collections.emptyIterator()
      override def close(): Unit = closed = true
    }
    assert(TrinoStatement.readRows(rows).contains(Nil))
    assert(closed)
  }

  test("decode all rows and close the page") {
    var closed = false
    val rows = new ResultRows {
      override def iterator(): JIterator[JList[Object]] =
        Arrays.asList[JList[Object]](
          Arrays.asList[Object](Int.box(7), "value", null)).iterator()
      override def close(): Unit = closed = true
    }
    assert(TrinoStatement.readRows(rows).contains(List(List(7, "value", null))))
    assert(closed)
  }

  test("close the page if decoding fails") {
    var closed = false
    val rows = new ResultRows {
      override def iterator(): JIterator[JList[Object]] =
        throw new IllegalStateException("decode failed")
      override def close(): Unit = closed = true
    }
    intercept[IllegalStateException](TrinoStatement.readRows(rows))
    assert(closed)
  }
}
