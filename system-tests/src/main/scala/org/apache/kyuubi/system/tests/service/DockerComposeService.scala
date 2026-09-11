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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.Duration

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory
import org.testcontainers.containers.{ComposeContainer, Container, ContainerState}
import org.testcontainers.containers.wait.strategy.{Wait, WaitStrategy}

import org.apache.kyuubi.system.tests.model.{Component, WaitParams}
import org.apache.kyuubi.system.tests.util.Utils
import org.apache.kyuubi.system.tests.util.constant.TimeoutConstants

class DockerComposeService(components: Seq[Component]) {

  private val log = LoggerFactory.getLogger(classOf[DockerComposeService])
  private val startupTimeout: Duration = Duration.ofMinutes(10)

  private val moduleRoot: Path = DockerComposeService.resolveModuleRoot()
  private val composeFile: File = moduleRoot.resolve(DockerComposeService.COMPOSE_FILE).toFile
  private val envFile: File = moduleRoot.resolve("env/.env").toFile

  @volatile private var compose: ComposeContainer = _

  def getModuleRoot: Path = moduleRoot

  def init(): Unit = {
    if (!composeFile.isFile) {
      throw new IllegalStateException(s"Compose file not found: ${composeFile.getAbsolutePath}")
    }
    DockerComposeService.seedKyuubiDefaultsRuntime(moduleRoot)

    val millisBeforeStart = System.currentTimeMillis()
    log.info(s"##### STARTING TEST CONTAINERS from ${composeFile.getAbsolutePath} #####")

    val container = new ComposeContainer(composeFile)
    DockerComposeService.loadEnvFile(envFile).foreach { case (k, v) =>
      container.withEnv(k, v)
    }

    components.foreach { component =>
      val wait: WaitStrategy =
        if (component.waitForHealthcheck) {
          Wait.forHealthcheck().withStartupTimeout(startupTimeout)
        } else {
          Wait.forListeningPort().withStartupTimeout(startupTimeout)
        }
      container.withExposedService(component.serviceName, component.port, wait)
    }

    container.withLocalCompose(true)
    container.start()
    compose = container
    log.info(
      s"##### TEST CONTAINERS HAVE STARTED IN ${(System.currentTimeMillis() - millisBeforeStart) / 1000} sec #####")
  }

  def getServiceHost(component: Component): String = {
    requireStarted()
    compose.getServiceHost(component.serviceName, component.port)
  }

  def getServicePort(component: Component): Int = {
    requireStarted()
    compose.getServicePort(component.serviceName, component.port)
  }

  def executeCommand(component: Component, command: String): Container.ExecResult = {
    try {
      val result = getContainer(component).execInContainer("/bin/bash", "-c", command)
      if (result.getExitCode != 0) {
        throw new IllegalStateException(
          s"""Command execution in the "${component.serviceName}" container failed with exit code
             | ${result.getExitCode}: ${result.getStderr}""".stripMargin.replace("\n", ""))
      }
      result
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new IllegalStateException(e)
      case NonFatal(e) =>
        throw new IllegalStateException(
          s"Failed to execute command in ${component.serviceName}: $command",
          e)
    }
  }

  def writeFile(component: Component, remotePath: String, content: String): Unit = {
    val encoded = java.util.Base64.getEncoder
      .encodeToString(content.getBytes(StandardCharsets.UTF_8))
    executeCommand(
      component,
      s"mkdir -p $$(dirname '$remotePath') && echo '$encoded' | base64 -d > '$remotePath'")
  }

  def stop(component: Component): Unit = {
    val containerState = getContainer(component)
    containerState.getDockerClient
      .stopContainerCmd(containerState.getContainerId)
      .withTimeout(10)
      .exec()
    Thread.sleep(TimeoutConstants.CONTAINER_OPERATION_DELAY_MS)
  }

  def start(component: Component): Unit = {
    val containerState = getContainer(component)
    containerState.getDockerClient
      .startContainerCmd(containerState.getContainerId)
      .exec()
    Thread.sleep(TimeoutConstants.CONTAINER_OPERATION_DELAY_MS)
  }

  def restart(
      component: Component,
      waitParams: WaitParams = TimeoutConstants.EXTENDED_WAIT_PARAMS): Unit = {
    val containerState = getContainer(component)
    log.info(s"Restarting container ${component.serviceName} (${containerState.getContainerId})")
    containerState.getDockerClient
      .restartContainerCmd(containerState.getContainerId)
      .exec()
    waitUntilHealthy(containerState, waitParams)
    log.info(s"Container ${component.serviceName} is healthy after restart")
  }

  def getContainerLogs(component: Component): String = {
    getContainer(component).getLogs
  }

  def stop(): Unit = {
    if (compose != null) {
      compose.stop()
      compose = null
    }
  }

  private def waitUntilHealthy(containerState: ContainerState, waitParams: WaitParams): Unit = {
    Utils.waitUntil(
      {
        if (!containerState.isHealthy) {
          throw new IllegalStateException(
            s"Container ${containerState.getContainerId} is not healthy yet")
        }
      },
      waitParams)
  }

  private def getContainer(component: Component): ContainerState = {
    requireStarted()
    compose
      .getContainerByServiceName(DockerComposeService.containerName(component.serviceName))
      .orElseThrow(() =>
        new IllegalStateException(
          s"Container couldn't be found for component ${component.serviceName}"))
  }

  private def requireStarted(): Unit = {
    if (compose == null) {
      throw new IllegalStateException("DockerComposeService has not been started")
    }
  }
}

object DockerComposeService {
  private val log = LoggerFactory.getLogger(classOf[DockerComposeService])
  private val SINGLE_CONTAINER_SUFFIX = "-1"
  private val NUMBERED_CONTAINER_REGEX = ".*-[0-9]+".r

  private[service] val COMPOSE_FILE =
    Paths.get("env/docker-compose-test.yml")
  private[service] val KYUUBI_DEFAULTS_BASELINE =
    Paths.get("env/conf/kyuubi-defaults.conf")
  private[service] val KYUUBI_DEFAULTS_RUNTIME =
    Paths.get("env/runtime/kyuubi-defaults.conf")

  def resolveModuleRoot(): Path = {
    val cwd = Paths.get("").toAbsolutePath.normalize()
    if (Files.isRegularFile(cwd.resolve(COMPOSE_FILE))) {
      cwd
    } else {
      val nested = cwd.resolve("system-tests")
      if (Files.isRegularFile(nested.resolve(COMPOSE_FILE))) {
        nested
      } else {
        throw new IllegalStateException(
          s"Cannot locate system-tests/$COMPOSE_FILE from working directory: $cwd")
      }
    }
  }

  def seedKyuubiDefaultsRuntime(moduleRoot: Path = resolveModuleRoot()): Path = {
    val baseline = moduleRoot.resolve(KYUUBI_DEFAULTS_BASELINE)
    val runtime = moduleRoot.resolve(KYUUBI_DEFAULTS_RUNTIME)
    if (!Files.isRegularFile(baseline)) {
      throw new IllegalStateException(s"Baseline conf not found: $baseline")
    }
    Files.createDirectories(runtime.getParent)
    Files.copy(baseline, runtime, StandardCopyOption.REPLACE_EXISTING)
    log.info(s"Seeded Kyuubi defaults runtime conf at $runtime")
    runtime
  }

  private def containerName(serviceName: String): String = {
    if (NUMBERED_CONTAINER_REGEX.pattern.matcher(serviceName).matches()) {
      serviceName
    } else {
      serviceName + SINGLE_CONTAINER_SUFFIX
    }
  }

  private def loadEnvFile(file: File): Map[String, String] = {
    if (!file.isFile) {
      log.warn(s"Env file not found: ${file.getAbsolutePath}")
      return Map.empty
    }
    val env = mutable.LinkedHashMap.empty[String, String]
    Files.readAllLines(file.toPath, StandardCharsets.UTF_8).asScala.foreach { line =>
      val trimmed = line.trim
      if (trimmed.nonEmpty && !trimmed.startsWith("#")) {
        val eq = trimmed.indexOf('=')
        if (eq > 0) {
          env.put(trimmed.substring(0, eq).trim, trimmed.substring(eq + 1).trim)
        }
      }
    }
    env.toMap
  }
}
