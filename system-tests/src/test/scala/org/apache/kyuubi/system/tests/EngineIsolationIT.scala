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

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import org.apache.kyuubi.system.tests.model.EngineProfile
import org.apache.kyuubi.system.tests.util.constant.ConfConstants._

class EngineIsolationIT extends KyuubiSystemContainerizedIT {

  private val VERSION_COMMAND: String = "SELECT version()"

  @Test
  def differentProfilesDoNotShareUserLevelEngine(): Unit = {
    confController.withOverlay(Map(
      ENGINE_SHARE_LEVEL -> SHARE_LEVEL_USER)) {
      val spark3 = kyuubiService.openConnection(
        Map(ENGINE_PROFILE -> EngineProfile.SPARK3.profileName))
      val spark4 = kyuubiService.openConnection(
        Map(ENGINE_PROFILE -> EngineProfile.SPARK4.profileName))
      try {
        val v3 = String.valueOf(
          kyuubiService.query(spark3, VERSION_COMMAND).head.values.head)
        val v4 = String.valueOf(
          kyuubiService.query(spark4, VERSION_COMMAND).head.values.head)
        assertTrue(
          v3.startsWith("3."),
          s"${EngineProfile.SPARK3.profileName} profile version: $v3")
        assertTrue(
          v4.startsWith("4."),
          s"${EngineProfile.SPARK4.profileName} profile version: $v4")

        val body = restService.listEngineProfiles(SHARE_LEVEL_USER)
        assertTrue(
          body.contains(EngineProfile.SPARK3.profileName),
          s"Expected ${EngineProfile.SPARK3.profileName} in engine/profile listing: $body")
        assertTrue(
          body.contains(EngineProfile.SPARK4.profileName),
          s"Expected ${EngineProfile.SPARK4.profileName} in engine/profile listing: $body")
      } finally {
        spark3.close()
        spark4.close()
      }
    }
  }
}
