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

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.spark.sql.execution.QueryExecution

import org.apache.kyuubi.Logging
import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.detailed.HdfsLineageLogger.{LINEAGE_FILE_NAME, PLAN_FILE_NAME, SQL_QUERY_HEADER}
import org.apache.kyuubi.util.JdbcUtils

class HdfsLineageLogger(
    val rootDir: String,
    config: Configuration,
    val lineageSerializer: LineageSerializer = new LineageJsonSerializer()) extends LineageLogger
  with Logging {
  private val fileSystem: FileSystem = FileSystem.get(config)

  override def log(execution: QueryExecution, lineage: Lineage): Unit = {
    val executionDir = getSessionDirectory(execution)

    if (!fileSystem.mkdirs(executionDir)) {
      throw new RuntimeException(s"Error creating directory $executionDir")
    }

    logQueryMetadata(executionDir, execution)
    logLineage(executionDir, lineage)
  }

  private def logQueryMetadata(executionDir: Path, execution: QueryExecution): Unit = {
    val path = new Path(executionDir, PLAN_FILE_NAME)

    val queryMetadata = execution.logical.origin
      .sqlText
      .map { sqlQuery =>
        s"""$SQL_QUERY_HEADER
           |$sqlQuery
           |
           |$execution""".stripMargin
      }.getOrElse(execution.toString())

    withNewFile(path) {
      IOUtils.write(queryMetadata, _, StandardCharsets.UTF_8)
    }
  }

  private def logLineage(executionDir: Path, lineage: Lineage): Unit = {
    val lineagePath = new Path(executionDir, LINEAGE_FILE_NAME)
    withNewFile(lineagePath) {
      IOUtils.write(lineageSerializer.serialize(lineage), _)
    }
  }

  private def withNewFile(filePath: Path)(action: FSDataOutputStream => Unit): Unit = {
    JdbcUtils.withCloseable(fileSystem.create(filePath)) {
      action(_)
    }
  }

  private def getSessionDirectory(execution: QueryExecution): Path = {
    val sparkAppName = execution.sparkSession.conf.get("spark.app.name")
    new Path(rootDir, new Path(sparkAppName, execution.id.toString))
  }
}

object HdfsLineageLogger {
  val PLAN_FILE_NAME = "query_metadata.txt"
  val LINEAGE_FILE_NAME = "lineage"
  val SQL_QUERY_HEADER = "== SQL Query =="
}
