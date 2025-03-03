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

package org.apache.spark.kyuubi.lineage

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, OptionalConfigEntry}

object OpenMetadataSparkConf {
  val OPEN_METADATA_SERVER_ADDRESS: OptionalConfigEntry[String] =
    ConfigBuilder("spark.kyuubi.plugin.lineage.openmetadata.server")
      .doc("Address of the OpenMetadata server")
      .version("1.9.2")
      .stringConf
      .createOptional

  val OPEN_METADATA_JWT: OptionalConfigEntry[String] =
    ConfigBuilder("spark.kyuubi.plugin.lineage.openmetadata.jwt")
      .doc("JsonWebToken for authenticating to the OpenMetadata server")
      .version("1.9.2")
      .stringConf
      .createOptional

  val OPEN_METADATA_PIPELINE_SERVICE_NAME: ConfigEntry[String] =
    ConfigBuilder("spark.kyuubi.plugin.lineage.openmetadata.pipelineServiceName")
      .doc("The name of the OpenMetadata pipeline service where " +
        "the spark pipelines will be created. If pipeline service " +
        "with specified name already exists it will be used, otherwise it will be created.")
      .version("1.9.2")
      .stringConf
      .createWithDefault("SparkOnKyuubi")

  val OPEN_METADATA_PIPELINE_NAME: ConfigEntry[String] =
    ConfigBuilder("spark.kyuubi.plugin.lineage.openmetadata.pipelineName")
      .doc("The name of the OpenMetadata pipeline. If pipeline " +
        "with specified name already exists it will be used, otherwise it will be created.")
      .version("1.9.2")
      .stringConf
      .createWithDefault("SparkOnKyuubi")

  val OPEN_METADATA_DATABASE_SERVICE_NAMES: ConfigEntry[Seq[String]] =
    ConfigBuilder("spark.kyuubi.plugin.lineage.openmetadata.databaseServiceNames")
      .doc("The comma separated list of database service names which contains the source tables " +
        "used in this job. If no services is provided then the search will be conducted " +
        "through all the database services available in the OpenMetadata server.")
      .version("1.9.2")
      .stringConf
      .toSequence
      .createWithDefault(Seq())
}
