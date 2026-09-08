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

import org.junit.jupiter.api.Assertions.{assertTrue, fail}
import org.junit.jupiter.api.Test

import org.apache.kyuubi.system.tests.util.constant.ConfConstants._

class ProfileFallbackIT extends KyuubiSystemContainerizedIT {

  @Test
  def unknownProfileFailsWithBaselineStrategy(): Unit = {
    var failed = false
    try {
      kyuubiService.query(
        Map(ENGINE_PROFILE -> UNKNOWN_PROFILE),
        "SELECT 1")
    } catch {
      case e: IllegalStateException =>
        failed = true
        assertTrue(
          Option(e.getCause).exists(_.getMessage.contains(UNKNOWN_PROFILE)) ||
            e.getMessage.contains(UNKNOWN_PROFILE) ||
            Option(e.getCause).exists(c =>
              Option(c.getCause).exists(_.getMessage.contains(UNKNOWN_PROFILE))),
          s"Expected unknown-profile error, got: ${e.getMessage}")
    }
    assertTrue(failed, "Expected FAIL strategy to reject unknown profile")
  }

  @Test
  def unknownProfileFallsBackUnderLogStrategy(): Unit = {
    confController.withOverlay(Map(
      PROFILES_UNKNOWN_STRATEGY -> UNKNOWN_STRATEGY_LOG)) {
      // LOG → no profile applied → legacy SPARK_HOME=/usr/lib/spark3 from kyuubi-env.sh
      val rows = kyuubiService.query(
        Map(ENGINE_PROFILE -> UNKNOWN_PROFILE),
        "SELECT version()")
      if (rows.isEmpty) {
        fail("Expected SELECT version() to succeed under LOG unknown-profile strategy")
      }
      val version = String.valueOf(rows.head.values.head)
      assertTrue(
        version.startsWith("3."),
        s"Expected legacy Spark 3 under LOG fallback, got: $version")
    }
  }
}
