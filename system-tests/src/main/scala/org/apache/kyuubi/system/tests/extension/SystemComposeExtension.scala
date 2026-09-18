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

package org.apache.kyuubi.system.tests.extension

import org.junit.jupiter.api.extension.{BeforeAllCallback, ExtensionContext}
import org.slf4j.LoggerFactory

import org.apache.kyuubi.system.tests.model.Component
import org.apache.kyuubi.system.tests.service.DockerComposeService

/**
 * Starts the system-tests Docker Compose stack once per JVM and keeps it for all IT classes.
 */
class SystemComposeExtension extends BeforeAllCallback {

  override def beforeAll(context: ExtensionContext): Unit = {
    SystemComposeExtension.ensureStarted()
  }
}

object SystemComposeExtension {
  private val log = LoggerFactory.getLogger(classOf[SystemComposeExtension])
  private val lock = new Object
  @volatile private var composeService: DockerComposeService = _

  def getComposeService: DockerComposeService = {
    ensureStarted()
    composeService
  }

  private def ensureStarted(): Unit = {
    if (composeService != null) {
      return
    }
    lock.synchronized {
      if (composeService == null) {
        val service = new DockerComposeService(Component.values)
        service.init()
        Runtime.getRuntime.addShutdownHook(new Thread(
          () => {
            log.info("Stopping system-tests Docker Compose stack")
            service.stop()
          },
          "kyuubi-system-tests-compose-shutdown"))
        composeService = service
      }
    }
  }
}
