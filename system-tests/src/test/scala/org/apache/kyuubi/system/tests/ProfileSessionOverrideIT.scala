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

package org.apache.kyuubi.system.tests

import io.qameta.allure.Feature
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.kyuubi.system.tests.model.EngineProfile
import org.apache.kyuubi.system.tests.util.constant.ConfConstants._

@Feature("Engine profile session override")
class ProfileSessionOverrideIT extends KyuubiSystemContainerizedIT {

  @Test
  def clientSparkConfOverridesProfileDefaults(): Unit = {
    val marker = "kyuubi-st-session-override"
    val rows = kyuubiService.query(
      Map(
        ENGINE_PROFILE -> EngineProfile.SPARK3.profileName,
        SPARK_APP_NAME -> marker),
      s"SET $SPARK_APP_NAME")
    val rendered = rows.map(_.values.map(String.valueOf).mkString("=")).mkString(";")
    assertTrue(
      rendered.contains(marker),
      s"Expected client $SPARK_APP_NAME=$marker to win over profile defaults, got: $rendered")
  }

  @Test
  def clientTrinoCatalogOverrideIsApplied(): Unit = {
    // Trino accepts SELECT 1 even with a missing session catalog; SHOW SCHEMAS uses it.
    var failed = false
    try {
      kyuubiService.query(
        Map(
          ENGINE_PROFILE -> EngineProfile.TRINO.profileName,
          TRINO_CONNECTION_CATALOG -> "no_such_catalog"),
        "SHOW SCHEMAS")
    } catch {
      case _: IllegalStateException => failed = true
    }
    assertTrue(failed, "Expected failure when overriding Trino catalog to a missing catalog")

    val ok = kyuubiService.query(
      Map(ENGINE_PROFILE -> EngineProfile.TRINO.profileName),
      "SELECT 1")
    assertEquals(1, ok.size)
  }
}
