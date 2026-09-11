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

package org.apache.kyuubi.system.tests

import io.qameta.allure.Epic
import org.junit.jupiter.api.{BeforeAll, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory

import org.apache.kyuubi.system.tests.extension.SystemComposeExtension
import org.apache.kyuubi.system.tests.service.{
  DockerComposeService,
  KyuubiConfController,
  KyuubiJdbcService,
  KyuubiRestService,
  PostgresJdbcService,
  WarehouseJdbcService
}

@Epic("Kyuubi system tests")
@ExtendWith(Array(classOf[SystemComposeExtension]))
@TestInstance(Lifecycle.PER_CLASS)
abstract class KyuubiSystemContainerizedIT {

  protected def composeService: DockerComposeService =
    KyuubiSystemContainerizedIT.composeService
  protected def kyuubiService: KyuubiJdbcService =
    KyuubiSystemContainerizedIT.kyuubiService
  protected def postgresService: PostgresJdbcService =
    KyuubiSystemContainerizedIT.postgresService
  protected def warehouseService: WarehouseJdbcService =
    KyuubiSystemContainerizedIT.warehouseService
  protected def confController: KyuubiConfController =
    KyuubiSystemContainerizedIT.confController
  protected def restService: KyuubiRestService =
    KyuubiSystemContainerizedIT.restService

  @BeforeAll
  def setupServices(): Unit = {
    KyuubiSystemContainerizedIT.ensureServices()
  }
}

object KyuubiSystemContainerizedIT {
  private val log = LoggerFactory.getLogger(classOf[KyuubiSystemContainerizedIT])
  private val lock = new Object

  @volatile private var _composeService: DockerComposeService = _
  @volatile private var _kyuubiService: KyuubiJdbcService = _
  @volatile private var _postgresService: PostgresJdbcService = _
  @volatile private var _warehouseService: WarehouseJdbcService = _
  @volatile private var _confController: KyuubiConfController = _
  @volatile private var _restService: KyuubiRestService = _

  def composeService: DockerComposeService = _composeService
  def kyuubiService: KyuubiJdbcService = _kyuubiService
  def postgresService: PostgresJdbcService = _postgresService
  def warehouseService: WarehouseJdbcService = _warehouseService
  def confController: KyuubiConfController = _confController
  def restService: KyuubiRestService = _restService

  def ensureServices(): Unit = {
    if (_kyuubiService != null) {
      return
    }
    lock.synchronized {
      if (_kyuubiService == null) {
        _composeService = SystemComposeExtension.getComposeService
        _kyuubiService = new KyuubiJdbcService(_composeService)
        _postgresService = new PostgresJdbcService(_composeService)
        _warehouseService = new WarehouseJdbcService(_composeService)
        _confController = new KyuubiConfController(_composeService)
        _restService = new KyuubiRestService(_composeService)
        log.info("System test JDBC/REST/conf services are ready")
      }
    }
  }
}
