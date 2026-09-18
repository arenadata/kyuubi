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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.slf4j.LoggerFactory

import org.apache.kyuubi.system.tests.model.Component
import org.apache.kyuubi.system.tests.util.constant.TimeoutConstants

class KyuubiConfController(compose: DockerComposeService) {

  private val log = LoggerFactory.getLogger(classOf[KyuubiConfController])
  private val moduleRoot: Path = compose.getModuleRoot
  private val baselinePath: Path =
    moduleRoot.resolve(DockerComposeService.KYUUBI_DEFAULTS_BASELINE)
  private val runtimePath: Path =
    moduleRoot.resolve(DockerComposeService.KYUUBI_DEFAULTS_RUNTIME)

  /**
   * Applies `overrides` on top of the baseline conf, restarts Kyuubi, runs `body`, then restores
   * the baseline. Serialized so concurrent IT classes do not race on the single gateway.
   */
  def withOverlay[T](overrides: Map[String, String])(body: => T): T = {
    KyuubiConfController.lock.synchronized {
      try {
        applyOverlay(overrides)
        body
      } finally {
        restoreBaseline()
      }
    }
  }

  def applyOverlay(overrides: Map[String, String]): Unit = {
    val merged = KyuubiConfController.mergeConf(readBaseline(), overrides)
    writeRuntime(merged)
    log.info(s"Applied Kyuubi conf overlay keys: ${overrides.keys.toSeq.sorted.mkString(", ")}")
    restartKyuubi()
  }

  def restoreBaseline(): Unit = {
    writeRuntime(readBaseline())
    log.info("Restored Kyuubi conf baseline")
    restartKyuubi()
  }

  private def restartKyuubi(): Unit = {
    compose.restart(Component.KYUUBI, TimeoutConstants.KYUUBI_RESTART_WAIT_PARAMS)
  }

  private def readBaseline(): String = {
    new String(Files.readAllBytes(baselinePath), StandardCharsets.UTF_8)
  }

  private def writeRuntime(content: String): Unit = {
    Files.createDirectories(runtimePath.getParent)
    Files.write(runtimePath, content.getBytes(StandardCharsets.UTF_8))
  }
}

object KyuubiConfController {
  private val lock = new Object

  private[service] def mergeConf(baseline: String, overrides: Map[String, String]): String = {
    if (overrides.isEmpty) {
      return baseline
    }
    val keys = overrides.keySet
    val filtered = baseline.linesIterator
      .filterNot { line =>
        val trimmed = line.trim
        trimmed.nonEmpty && !trimmed.startsWith("#") &&
        keys.exists(k => trimmed.startsWith(k + "=") || trimmed.startsWith(k + " "))
      }
      .mkString("\n")
    val overlayBlock = overrides.toSeq.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString("\n")
    s"$filtered\n\n# system-tests overlay\n$overlayBlock\n"
  }
}
