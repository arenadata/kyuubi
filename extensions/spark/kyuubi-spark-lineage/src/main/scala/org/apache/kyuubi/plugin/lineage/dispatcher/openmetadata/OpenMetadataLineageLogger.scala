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

package org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata

import org.apache.spark.sql.execution.QueryExecution

import org.apache.kyuubi.plugin.lineage.{ColumnLineage, Lineage}
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.client.OpenMetadataClient
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.model.{EntityColumnLineage, LineageDetails, OpenMetadataEntity}

class OpenMetadataLineageLogger(
    val openMetadataClient: OpenMetadataClient,
    val databaseServiceNames: Seq[String],
    pipelineServiceName: String,
    val pipelineName: String = null) {

  private lazy val pipelineService = openMetadataClient
    .createPipelineServiceIfNotExists(pipelineServiceName)

  def log(execution: QueryExecution, lineage: Lineage): Unit = {
    val inputTablesEntities = getTableNameToEntityMapping(lineage.inputTables)
    val outputTableEntities = getTableNameToEntityMapping(lineage.outputTables)

    val pipeline = openMetadataClient.createPipelineIfNotExists(
      pipelineService.fullyQualifiedName,
      getPipelineName(execution))

    val entityColumnLineage = buildEntityColumnsLineage(
      inputTablesEntities,
      outputTableEntities,
      lineage.columnLineage)

    for (fromTable <- inputTablesEntities.values;
      toTable <- outputTableEntities.values) {
      val lineageDetails = LineageDetails(
        pipeline = pipeline.toReference,
        description = formatAsCode(execution.sparkPlan.toString()),
        sqlQuery = execution.logical.origin.sqlText.orNull,
        columnsLineage = getEntityColumnsLineage(fromTable, toTable, entityColumnLineage))

      openMetadataClient.addLineage(fromTable, toTable, lineageDetails)
    }
  }

  private def getEntityColumnsLineage(
      inputTable: OpenMetadataEntity,
      outputTable: OpenMetadataEntity,
      outputTableToColumnsLineage: Map[String, Seq[EntityColumnLineage]])
      : Seq[EntityColumnLineage] = {
    outputTableToColumnsLineage.get(outputTable.fullyQualifiedName)
      .map { columnsLineage =>
        columnsLineage.map(toInputTableColumnLineage(inputTable, _))
          .filter(_.isDefined)
          .map(_.get)
      }.getOrElse {
        throw new IllegalArgumentException(
          s"Malformed lineages map: $outputTableToColumnsLineage. " +
            s"Key for table ${inputTable.fullyQualifiedName} not found.")
      }
  }

  private def toInputTableColumnLineage(
      inputTable: OpenMetadataEntity,
      columnLineage: EntityColumnLineage): Option[EntityColumnLineage] = {
    val inputTableColumnsLineage = columnLineage.fromColumns
      .filter(inputTable.fullyQualifiedName == extractTableName(_))

    if (inputTableColumnsLineage.isEmpty) {
      None
    } else {
      Some(EntityColumnLineage(
        columnLineage.toColumn,
        inputTableColumnsLineage))
    }
  }

  private def buildEntityColumnsLineage(
      inputEntities: Map[String, OpenMetadataEntity],
      outputEntities: Map[String, OpenMetadataEntity],
      columnLineage: List[ColumnLineage]): Map[String, Seq[EntityColumnLineage]] = {
    columnLineage.map {
      buildEntityColumnLineage(inputEntities, outputEntities, _)
    }.groupBy { lineage => extractTableName(lineage.toColumn) }
  }

  private def buildEntityColumnLineage(
      inputEntities: Map[String, OpenMetadataEntity],
      outputEntities: Map[String, OpenMetadataEntity],
      lineage: ColumnLineage): EntityColumnLineage = {
    val outputEntityColumn = mapColumnName(lineage.column, outputEntities)
    val inputEntityColumns = lineage.originalColumns
      .map(mapColumnName(_, inputEntities))
      .toSeq

    EntityColumnLineage(outputEntityColumn, inputEntityColumns)
  }

  private def mapColumnName(
      fullColumnName: String,
      entityMappings: Map[String, OpenMetadataEntity]): String = {
    val (fullTableName, columnName) = splitColumnName(fullColumnName)

    entityMappings.get(fullTableName)
      .map(_.fullyQualifiedName)
      .map { fqn => s"$fqn.$columnName" }
      .getOrElse {
        throw new IllegalArgumentException(s"Malformed entities map $entityMappings." +
          s"Spark table with name $fullTableName not found")
      }
  }

  private def getTableNameToEntityMapping(
      sparkTableNames: List[String]): Map[String, OpenMetadataEntity] = {
    sparkTableNames.map {
      table => (table, getTableEntity(table))
    }.toMap
  }

  private def getTableEntity(tableName: String): OpenMetadataEntity = {
    val maybeTableEntity = if (databaseServiceNames.isEmpty) {
      searchTableEntityGlobally(tableName)
    } else {
      searchTableEntityInServices(tableName)
    }

    maybeTableEntity.getOrElse {
      throw new IllegalArgumentException(
        s"OpenMetadata Entity for table $tableName not found")
    }
  }

  private def searchTableEntityGlobally(tableName: String): Option[OpenMetadataEntity] = {
    val searchPattern = getTableEntityPattern(tableName)
    openMetadataClient.getTableEntity(searchPattern)
  }

  private def searchTableEntityInServices(tableName: String): Option[OpenMetadataEntity] = {
    databaseServiceNames
      .view
      .map { dbService =>
        val searchPattern = getTableEntityPattern(tableName, dbService)
        openMetadataClient.getTableEntity(searchPattern)
      }
      .find(_.isDefined)
      .flatten
  }

  private def getTableEntityPattern(tableName: String, prefixes: String*): String = {
    val tablePattern = tableName.split('.') match {
      case Array(_, db, table) =>
        s"*$db.$table"
      case _ =>
        throw new IllegalArgumentException("Unsupported table name format")
    }

    if (prefixes.isEmpty) {
      tablePattern
    } else {
      s"${prefixes.mkString(".")}.$tablePattern"
    }
  }

  private def getPipelineName(execution: QueryExecution): String = {
    pipelineName match {
      case null =>
        val context = execution.sparkSession.sparkContext
        s"${context.appName}_${context.sparkUser}"
      case name => name
    }
  }

  private def splitColumnName(fullColumnName: String): (String, String) = {
    fullColumnName.lastIndexOf('.') match {
      case -1 => throw new IllegalArgumentException(
          s"Wrong format of Spark column full name: $fullColumnName")
      case idx => (fullColumnName.substring(0, idx), fullColumnName.substring(idx + 1))
    }
  }

  private def extractTableName(fullColumnName: String): String =
    splitColumnName(fullColumnName)._1

  private def formatAsCode(rawText: String): String =
    s"<pre><code>$rawText</code></pre>"
}
