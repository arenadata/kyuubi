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

package org.apache.kyuubi.plugin.lineage.detailed

import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Using}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.LineageConf.{DEFAULT_CATALOG, DISPATCHERS}
import org.apache.spark.sql.SparkListenerExtensionTest
import org.apache.spark.sql.execution.QueryExecution

import org.apache.kyuubi.{KyuubiFunSuite, WithSimpleDFSService}
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.detailed.HdfsLineageLogger.{LINEAGE_FILE_NAME, PLAN_FILE_NAME}
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.SPARK_RUNTIME_VERSION

class HdfsDetailedEventHandlerSuite extends KyuubiFunSuite
  with WithSimpleDFSService
  with SparkListenerExtensionTest {

  val catalogName: String = if (SPARK_RUNTIME_VERSION <= "3.1") {
    "org.apache.spark.sql.connector.InMemoryTableCatalog"
  } else {
    "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"
  }

  override protected val catalogImpl: String = "hive"

  private var hdfsClient: FileSystem = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    hdfsClient = FileSystem.get(getHadoopConf)
  }

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.sql.catalog.v2_catalog", catalogName)
      .set(
        "spark.sql.queryExecutionListeners",
        "org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener")
      .set(DISPATCHERS.key, "KYUUBI_DETAILED_EVENT")
      .set("spark.app.name", "kyuubi_app")
  }

  test("lineage event was captured") {
    val eventHandler = registerLineageEventHandler(1)

    withTable("test_table0") { _ =>
      spark.sql("create table test_table0(a string, b string)")
      spark.sql("select a as col0, b as col1 from test_table0").collect()

      eventHandler.expectedEventsLatch.await(20, TimeUnit.SECONDS)

      assert(eventHandler.handledEvents.size == 1)

      val actualEvent = eventHandler.handledEvents(0)
      val expectedLineage = Lineage(
        List(s"$DEFAULT_CATALOG.default.test_table0"),
        List(),
        List(
          ("col0", Set(s"$DEFAULT_CATALOG.default.test_table0.a")),
          ("col1", Set(s"$DEFAULT_CATALOG.default.test_table0.b"))))
      assert(actualEvent._2 == expectedLineage)

      val executionDir = s"/test/lineage/kyuubi_app/${actualEvent._1.id}/"
      assert(hdfsClient.exists(new Path(executionDir, LINEAGE_FILE_NAME)))
      assert(hdfsClient.exists(new Path(executionDir, PLAN_FILE_NAME)))
    }
  }

  test("lineage event was written to HDFS") {
    val eventHandler = registerLineageEventHandler(1)

    withTable("test_table0") { _ =>
      spark.sql("create table test_table0(a string, b string)")
      val sqlQuery = "select a as col0, b as col1 from test_table0"
      spark.sql(sqlQuery).collect()

      eventHandler.expectedEventsLatch.await(20, TimeUnit.SECONDS)
      assert(eventHandler.handledEvents.size == 1)

      val lineageEvent = eventHandler.handledEvents(0)
      val executionDir = s"/test/lineage/kyuubi_app/${lineageEvent._1.id}/"

      val actualPlan = readUtf8(executionDir, PLAN_FILE_NAME)
      val expectedPlan =
        s"""${HdfsLineageLogger.SQL_QUERY_HEADER}
           |$sqlQuery
           |
           |${lineageEvent._1}""".stripMargin
      assert(expectedPlan == actualPlan)

      val expectedLineage =
        """{
          |  "inputTables" : [ "spark_catalog.default.test_table0" ],
          |  "outputTables" : [ ],
          |  "columnLineage" : [ {
          |    "column" : "col0",
          |    "originalColumns" : [ "spark_catalog.default.test_table0.a" ]
          |  }, {
          |    "column" : "col1",
          |    "originalColumns" : [ "spark_catalog.default.test_table0.b" ]
          |  } ]
          |}""".stripMargin

      val actualLineage = readUtf8(executionDir, LINEAGE_FILE_NAME)
      assert(actualLineage == expectedLineage)
    }
  }

  private def readUtf8(parent: String, file: String): String = {
    Using(hdfsClient.open(new Path(parent, file))) {
      IOUtils.readFullyToByteArray(_)
    } match {
      case Success(bytes) => new String(bytes, StandardCharsets.UTF_8)
      case Failure(exception) => throw exception
    }
  }

  private def registerLineageEventHandler(
      expectedEvents: Int,
      eventRootPath: String = "/test/lineage"): LineageLoggerWrapper = {
    val eventLogger = new LineageLoggerWrapper(
      new HdfsLineageLogger(eventRootPath, getHadoopConf),
      new CountDownLatch(expectedEvents))
    EventBus.register(new KyuubiDetailedEventHandler(eventLogger))
    eventLogger
  }

  private class LineageLoggerWrapper(
      val delegate: LineageLogger,
      val expectedEventsLatch: CountDownLatch) extends LineageLogger {

    val handledEvents: ArrayBuffer[(QueryExecution, Lineage)] = new ArrayBuffer()

    override def log(execution: QueryExecution, lineage: Lineage): Unit = {
      try {
        delegate.log(execution, lineage)
      } finally {
        expectedEventsLatch.countDown()
        handledEvents += Tuple2(execution, lineage)
      }
    }
  }
}
