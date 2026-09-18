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

import io.qameta.allure.Feature
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import org.apache.kyuubi.system.tests.model.EngineProfile
import org.apache.kyuubi.system.tests.util.constant.ConfConstants._

@Feature("Session conf policy")
class SessionConfPolicyIT extends KyuubiSystemContainerizedIT {

  @Test
  def ignoreListStripsClientKey(): Unit = {
    val marker = "should-be-ignored"
    confController.withOverlay(Map(
      SESSION_CONF_IGNORE_LIST -> SPARK_APP_NAME)) {
      val rows = kyuubiService.query(
        Map(
          ENGINE_PROFILE -> EngineProfile.SPARK3.profileName,
          SPARK_APP_NAME -> marker),
        s"SET $SPARK_APP_NAME")
      val rendered = rows.map(_.values.map(String.valueOf).mkString("=")).mkString(";")
      assertTrue(
        !rendered.contains(marker),
        s"Ignored $SPARK_APP_NAME should not reach the engine, got: $rendered")
    }
  }

  @Test
  def restrictListRejectsClientKey(): Unit = {
    confController.withOverlay(Map(
      SESSION_CONF_RESTRICT_LIST -> SPARK_MASTER)) {
      var failed = false
      try {
        kyuubiService.query(
          Map(
            ENGINE_PROFILE -> EngineProfile.SPARK3.profileName,
            SPARK_MASTER -> "spark://evil-host:7077"),
          "SELECT 1")
      } catch {
        case _: IllegalStateException => failed = true
      }
      assertTrue(failed, s"Expected restrict.list to reject $SPARK_MASTER session param")
    }
  }
}
