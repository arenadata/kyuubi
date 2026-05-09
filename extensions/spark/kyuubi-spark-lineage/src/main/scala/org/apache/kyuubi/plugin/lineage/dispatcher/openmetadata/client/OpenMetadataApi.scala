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

package org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.client

import feign.{Headers, Param, RequestLine}

import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.model._

trait OpenMetadataApi {

  @RequestLine(
    "GET /api/v1/search/fieldQuery?fieldName={fieldName}&fieldValue={fieldValue}&index={index}")
  @Headers(Array("Accept: application/json"))
  def searchEntitiesWithSpecificFieldAndValue(
      @Param("fieldName") fieldName: String,
      @Param("fieldValue") fieldValue: String,
      @Param("index") index: String): OpenMetadataEntitySearchResponse

  @RequestLine("PUT /api/v1/pipelines")
  @Headers(Array("Content-Type: application/json", "Accept: application/json"))
  def createOrUpdatePipeline(createPipeline: CreatePipelineRequest): OpenMetadataEntityInfo

  @RequestLine("PUT /api/v1/services/pipelineServices")
  @Headers(Array("Content-Type: application/json", "Accept: application/json"))
  def createOrUpdatePipelineService(
      createPipelineService: CreatePipelineServiceRequest): OpenMetadataEntityInfo

  @RequestLine("PUT /api/v1/lineage")
  @Headers(Array("Content-Type: application/json", "Accept: application/json"))
  def addLineageEdge(addLineage: AddLineageRequest): Unit
}
