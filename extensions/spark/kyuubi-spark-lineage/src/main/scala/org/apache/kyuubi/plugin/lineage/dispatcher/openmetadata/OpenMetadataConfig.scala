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

import org.apache.spark.kyuubi.lineage.OpenMetadataSparkConf.{OPEN_METADATA_DATABASE_SERVICE_NAMES, OPEN_METADATA_JWT, OPEN_METADATA_PIPELINE_NAME, OPEN_METADATA_PIPELINE_SERVICE_NAME, OPEN_METADATA_SERVER_ADDRESS}
import org.apache.spark.kyuubi.lineage.SparkContextHelper

case class OpenMetadataConfig(
    serverAddress: String,
    pipelineServiceName: String,
    pipelineName: String,
    databaseServiceNames: Seq[String] = Seq(),
    jwt: String)

object OpenMetadataConfig {
  def apply(): OpenMetadataConfig = {
    OpenMetadataConfig(
      SparkContextHelper.getConf(OPEN_METADATA_SERVER_ADDRESS).getOrElse {
        throw new IllegalArgumentException(
          s"${OPEN_METADATA_SERVER_ADDRESS.key} option shouldn't be empty")
      },
      SparkContextHelper.getConf(OPEN_METADATA_PIPELINE_SERVICE_NAME),
      SparkContextHelper.getConf(OPEN_METADATA_PIPELINE_NAME),
      SparkContextHelper.getConf(OPEN_METADATA_DATABASE_SERVICE_NAMES),
      SparkContextHelper.getConf(OPEN_METADATA_JWT).orNull)
  }
}
