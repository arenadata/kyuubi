/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.system.tests.service

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

import org.slf4j.LoggerFactory

import org.apache.kyuubi.system.tests.model.Component

class KyuubiRestService(compose: DockerComposeService) {

  private val log = LoggerFactory.getLogger(classOf[KyuubiRestService])
  private val http = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .build()

  def baseUrl: String = {
    val host = compose.getServiceHost(Component.KYUUBI_REST)
    val port = compose.getServicePort(Component.KYUUBI_REST)
    s"http://$host:$port"
  }

  def get(pathAndQuery: String): String = {
    val uri = URI.create(s"$baseUrl$pathAndQuery")
    log.info(s"REST GET $uri")
    val request = HttpRequest.newBuilder(uri)
      .timeout(Duration.ofSeconds(30))
      .GET()
      .build()
    val response = http.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new IllegalStateException(
        s"REST GET $uri failed: HTTP ${response.statusCode()} ${response.body()}")
    }
    response.body()
  }

  def listEngineProfiles(shareLevel: String): String = {
    get(s"/api/v1/admin/engine/profile?sharelevel=$shareLevel")
  }
}
