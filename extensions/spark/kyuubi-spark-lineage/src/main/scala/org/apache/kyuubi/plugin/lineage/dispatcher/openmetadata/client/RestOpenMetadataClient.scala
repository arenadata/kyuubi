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

import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.client.RestOpenMetadataClient._
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.model._

class RestOpenMetadataClient(apiBuilder: OpenMetadataApiBuilder) extends OpenMetadataClient {

  private lazy val openMetadataApi: OpenMetadataApi = apiBuilder.build()

  override def getTableEntity(fullyQualifiedNameTemplate: String): Option[OpenMetadataEntity] = {
    val searchResult = openMetadataApi.searchEntitiesWithSpecificFieldAndValue(
      TABLE_ENTITY_FQN_FIELD,
      fullyQualifiedNameTemplate,
      TABLE_ENTITY_SEARCH_INDEX)

    searchResult.hits.hits
      .headOption
      .map(_.entity)
  }

  override def addLineage(
      from: OpenMetadataEntity,
      to: OpenMetadataEntity,
      lineageDetails: LineageDetails): Unit = {
    val request = AddLineageRequest(
      LineageEdge(
        fromEntity = from.toReference,
        toEntity = to.toReference,
        lineageDetails = lineageDetails))
    openMetadataApi.addLineageEdge(request)
  }

  override def createPipelineServiceIfNotExists(pipelineService: String): OpenMetadataEntity = {
    val createPipelineRequest = CreatePipelineServiceRequest(
      name = pipelineService,
      serviceType = PIPELINE_SERVICE_TYPE)
    openMetadataApi.createOrUpdatePipelineService(createPipelineRequest)
      .withType(PIPELINE_SERVICE_ENTITY_TYPE)
  }

  override def createPipelineIfNotExists(
      pipelineService: String,
      pipeline: String): OpenMetadataEntity = {
    val createPipelineRequest = CreatePipelineRequest(
      service = pipelineService,
      name = pipeline)

    openMetadataApi.createOrUpdatePipeline(createPipelineRequest)
      .withType(PIPELINE_ENTITY_TYPE)
  }
}

object RestOpenMetadataClient {
  private val TABLE_ENTITY_FQN_FIELD = "fullyQualifiedName"
  private val TABLE_ENTITY_SEARCH_INDEX = "table_search_index"

  private val PIPELINE_ENTITY_TYPE = "pipeline"
  private val PIPELINE_SERVICE_ENTITY_TYPE = "pipelineService"
  private val PIPELINE_SERVICE_TYPE = "Spark"

  def apply(serverAddress: String, jwt: String): RestOpenMetadataClient = {
    val authTokenProvider = jwt match {
      case null => NoOpAuthenticationTokenProvider
      case token => new StaticAuthenticationTokenProvider(token)
    }

    new RestOpenMetadataClient(
      new DefaultOpenMetadataApiBuilder(serverAddress, authTokenProvider))
  }
}
