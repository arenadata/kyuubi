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

package org.apache.kyuubi.plugin.lineage.dispatcher.atlas

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.LineageConf.DEFAULT_CATALOG
import org.apache.spark.sql.SparkListenerExtensionTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.QueryExecution
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.dispatcher.atlas.OpenMetadataLineageDispatcherSuite.{TEST_PIPELINE_SERVICE_NAME, TEST_SPARK_APP_NAME}
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.OpenMetadataLineageLogger
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.client.OpenMetadataClient
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.model.{LineageDetails, OpenMetadataEntity}
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.SPARK_RUNTIME_VERSION

class OpenMetadataLineageDispatcherSuite extends KyuubiFunSuite with SparkListenerExtensionTest {
  private val catalogName = if (SPARK_RUNTIME_VERSION <= "3.1") {
    "org.apache.spark.sql.connector.InMemoryTableCatalog"
  } else {
    "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"
  }

  override protected val catalogImpl: String = "hive"

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.sql.catalog.v2_catalog", catalogName)
      .set("spark.app.name", TEST_SPARK_APP_NAME)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("create database if not exists test_db")
  }

  override def afterAll(): Unit = {
    spark.sql("drop database if exists test_db")
    spark.stop()
    super.afterAll()
  }

  test("log lineage with no specified db services") {
    val entitiesMap = Seq("default.test_table0", "default.test_table1")
      .map { table => ("*" + table, entity(table)) }
      .toMap

    val openMetadataClient = new MockOpenMetadataClient(entitiesMap)
    val lineageLogger = new OpenMetadataLineageLogger(
      openMetadataClient,
      Seq(),
      TEST_PIPELINE_SERVICE_NAME)

    withTable("test_table0") { _ =>
      withTable("test_table1") { _ =>
        spark.sql("create table test_table0(a string, b int, c int)")
        spark.sql("create table test_table1(a string, d int)")

        val query = "insert into test_table1 select a, b + c as d from test_table0"

        val lineage = Lineage(
          List(s"$DEFAULT_CATALOG.default.test_table0"),
          List(s"$DEFAULT_CATALOG.default.test_table1"),
          List(
            (
              s"$DEFAULT_CATALOG.default.test_table1.a",
              Set(s"$DEFAULT_CATALOG.default.test_table0.a")),
            (
              s"$DEFAULT_CATALOG.default.test_table1.d",
              Set(
                s"$DEFAULT_CATALOG.default.test_table0.b",
                s"$DEFAULT_CATALOG.default.test_table0.c"))))

        val queryExecution = spark.sql(query).queryExecution
        lineageLogger.log(queryExecution, lineage)

        eventually(Timeout(5.seconds)) {
          assert(openMetadataClient.lineages.nonEmpty)
        }

        assertClientRequests(openMetadataClient, lineage, query)
      }
    }
  }

  test("log lineage with specified db services") {
    val entitiesMap = Map(
      "dbService1.*test_db.tb1" -> entity("test_db.tb1"),
      "dbService2.*test_db.tb2" -> entity("test_db.tb2"),
      "dbService2.*test_db.dest" -> entity("test_db.dest"))

    val openMetadataClient = new MockOpenMetadataClient(entitiesMap)
    val lineageLogger = new OpenMetadataLineageLogger(
      openMetadataClient,
      Seq("otherDbService", "dbService1", "dbService2"),
      TEST_PIPELINE_SERVICE_NAME)

    withTable("test_db.tb1") { _ =>
      withTable("test_db.tb2") { _ =>
        withTable("test_db.dest") { _ =>
          spark.sql("create table test_db.tb1(col1 string, col2 string, col3 string)")
          spark.sql("create table test_db.tb2(col1 string, col2 string, col3 string)")
          spark.sql("create table test_db.dest(a1 string, b1 string, ab1 string, ab2 string)")

          val query =
            """
              |insert into test_db.dest
              |select t1.col1 as a1, t2.col1 as b1, concat(t1.col1, t2.col1) as ab1,
              |concat(concat(t1.col1, t2.col2), concat(t1.col2, t2.col3)) as ab2
              |from test_db.tb1 t1 join test_db.tb2 t2
              |on t1.col1 = t2.col1
              |""".stripMargin

          val queryExecution = spark.sql(query).queryExecution

          val lineage = Lineage(
            List(s"$DEFAULT_CATALOG.test_db.tb1", s"$DEFAULT_CATALOG.test_db.tb2"),
            List(s"$DEFAULT_CATALOG.test_db.dest"),
            List(
              (s"$DEFAULT_CATALOG.test_db.dest.a1", Set(s"$DEFAULT_CATALOG.test_db.tb1.col1")),
              (s"$DEFAULT_CATALOG.test_db.dest.b1", Set(s"$DEFAULT_CATALOG.test_db.tb2.col1")),
              (
                s"$DEFAULT_CATALOG.test_db.dest.ab1",
                Set(s"$DEFAULT_CATALOG.test_db.tb1.col1", s"$DEFAULT_CATALOG.test_db.tb2.col1")),
              (
                s"$DEFAULT_CATALOG.test_db.dest.ab2",
                Set(
                  s"$DEFAULT_CATALOG.test_db.tb1.col1",
                  s"$DEFAULT_CATALOG.test_db.tb1.col2",
                  s"$DEFAULT_CATALOG.test_db.tb2.col2",
                  s"$DEFAULT_CATALOG.test_db.tb2.col3"))))

          lineageLogger.log(queryExecution, lineage)

          eventually(Timeout(5.seconds)) {
            assert(openMetadataClient.lineages.size == 2)
          }

          assertClientRequests(openMetadataClient, lineage, query)
        }
      }
    }
  }

  test("fail if entity for src table not found with no specified db services") {
    val tableEntities: Map[String, OpenMetadataEntity] = Map()
    val databaseServiceNames: Seq[String] = Seq()

    checkFailIfNoTableFound(tableEntities, databaseServiceNames)
  }

  test("fail if entity for src table not found with specified db services") {
    val tableEntities: Map[String, OpenMetadataEntity] = Map()
    val databaseServiceNames: Seq[String] = Seq("dbService1", "dbService2")

    checkFailIfNoTableFound(tableEntities, databaseServiceNames)
  }

  test("fail if entity for dest table not found with no specified db services") {
    val tableEntities: Map[String, OpenMetadataEntity] = Map(
      "*default.dest" -> entity("default.dest"))
    val databaseServiceNames: Seq[String] = Seq()

    checkFailIfNoTableFound(tableEntities, databaseServiceNames)
  }

  test("fail if entity for dest table not found with specified db services") {
    val tableEntities: Map[String, OpenMetadataEntity] = Map(
      "*default.dest" -> entity("default.dest"))
    val databaseServiceNames: Seq[String] = Seq("dbService1", "dbService2")

    checkFailIfNoTableFound(tableEntities, databaseServiceNames)
  }

  private def checkFailIfNoTableFound(
      tableEntities: Map[String, OpenMetadataEntity],
      databaseServiceNames: Seq[String]): Unit = {
    val openMetadataClient = new MockOpenMetadataClient(tableEntities)
    val lineageLogger = new OpenMetadataLineageLogger(
      openMetadataClient,
      databaseServiceNames,
      TEST_PIPELINE_SERVICE_NAME)

    val execution = new QueryExecution(spark, LocalRelation())
    val lineage = Lineage(
      List(s"$DEFAULT_CATALOG.default.src"),
      List(s"$DEFAULT_CATALOG.default.dest"),
      List())

    val exception = intercept[IllegalArgumentException] {
      lineageLogger.log(execution, lineage)
    }
    assert(exception.getMessage.contains("not found"))
  }

  private def assertClientRequests(
      client: MockOpenMetadataClient,
      lineage: Lineage,
      query: String): Unit = {
    assert(client.pipelineServices.size == 1)
    assert(client.pipelineServices.contains(TEST_PIPELINE_SERVICE_NAME))

    assert(client.pipelines.size == 1)
    assert(client.pipelines.keys.exists(_.startsWith(TEST_SPARK_APP_NAME)))

    assert(client.lineages.nonEmpty)
    assertLineage(client, lineage, query)
  }

  private def assertLineage(
      client: MockOpenMetadataClient,
      lineage: Lineage,
      query: String): Unit = {
    for (src <- lineage.inputTables;
      dest <- lineage.outputTables) {
      val lineageEdge = client.lineages.get(
        LineageKey(removeCatalog(src), removeCatalog(dest)))
      assert(lineageEdge.isDefined)
      assert(lineageEdge.get.sqlQuery == query)

      val columnLineages = lineage.columnLineage
        .filter(_.column.startsWith(dest))

      for (columnLineage <- columnLineages) {
        val actualSrcColumns = lineageEdge.get.columnsLineage
          .find(_.toColumn == removeCatalog(columnLineage.column))
          .map(_.fromColumns)
          .getOrElse(Seq())

        val expectedSrcColumns = columnLineage.originalColumns
          .filter(_.startsWith(src))
          .map(removeCatalog)

        assert(expectedSrcColumns == actualSrcColumns.toSet)
      }
    }
  }

  private def removeCatalog(tableName: String): String = {
    tableName.replace(s"$DEFAULT_CATALOG.", "")
  }

  private def entity(name: String, entityType: String = "table") =
    OpenMetadataEntity(UUID.randomUUID(), name, entityType)

  class MockOpenMetadataClient(tableEntities: Map[String, OpenMetadataEntity])
    extends OpenMetadataClient {
    val pipelineServices: mutable.Map[String, OpenMetadataEntity] = mutable.Map()
    val pipelines: mutable.Map[String, OpenMetadataEntity] = mutable.Map()
    val lineages: mutable.Map[LineageKey, LineageDetails] = mutable.Map()

    override def createPipelineServiceIfNotExists(pipelineService: String): OpenMetadataEntity = {
      pipelineServices.getOrElseUpdate(
        pipelineService,
        entity(pipelineService, "pipelineService"))
    }

    override def createPipelineIfNotExists(
        pipelineService: String,
        pipeline: String): OpenMetadataEntity = {

      pipelines.getOrElseUpdate(
        pipeline,
        entity(pipeline, "pipeline"))
    }

    override def getTableEntity(fullyQualifiedNameTemplate: String): Option[OpenMetadataEntity] = {
      tableEntities.get(fullyQualifiedNameTemplate)
    }

    override def addLineage(
        from: OpenMetadataEntity,
        to: OpenMetadataEntity,
        lineageDetails: LineageDetails): Unit = {
      lineages(LineageKey(from.fullyQualifiedName, to.fullyQualifiedName)) = lineageDetails
    }
  }

  case class LineageKey(from: String, to: String)
}

object OpenMetadataLineageDispatcherSuite {
  val TEST_SPARK_APP_NAME = "open_metadata_test_app"
  val TEST_PIPELINE_SERVICE_NAME = "testSparkLineage"
}
