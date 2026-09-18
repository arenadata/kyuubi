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
import org.junit.jupiter.api.Assertions.{assertTrue, fail}
import org.junit.jupiter.api.Test

import org.apache.kyuubi.system.tests.model.EngineProfile
import org.apache.kyuubi.system.tests.util.constant.ConfConstants._

@Feature("Engine profile resolve")
class ProfileResolveIT extends KyuubiSystemContainerizedIT {

  private val ALICE = "alice"

  @Test
  def explicitProfileSelectsSpark4(): Unit = {
    val version = sparkVersion(Map(ENGINE_PROFILE -> EngineProfile.SPARK4.profileName))
    assertTrue(version.startsWith("4."), s"Expected Spark 4.x, got: $version")
  }

  @Test
  def typeDefaultResolvesSpark3WithoutProfileParam(): Unit = {
    // Server default engine type is SPARK_SQL; profile.default=spark3.
    val version = sparkVersion(Map(ENGINE_TYPE -> ENGINE_TYPE_SPARK_SQL))
    assertTrue(version.startsWith("3."), s"Expected Spark 3.x default profile, got: $version")
  }

  @Test
  def userDefaultProfileViaConfOverlay(): Unit = {
    confController.withOverlay(Map(
      userDefaultProfileKey(ALICE) -> EngineProfile.SPARK4.profileName)) {
      val version = sparkVersion(Map.empty, user = ALICE)
      assertTrue(
        version.startsWith("4."),
        s"Expected user-default ${EngineProfile.SPARK4.profileName} for $ALICE, got: $version")
    }
  }

  @Test
  def groupDefaultProfileViaConfOverlay(): Unit = {
    confController.withOverlay(Map(
      HADOOP_USER_GROUP_STATIC_MAPPING -> s"$ALICE=$TEST_GROUP_ANALYSTS",
      groupDefaultProfileKey(TEST_GROUP_ANALYSTS) -> EngineProfile.SPARK4.profileName)) {
      val version = sparkVersion(Map.empty, user = ALICE)
      assertTrue(
        version.startsWith("4."),
        s"Expected group-default ${EngineProfile.SPARK4.profileName} for " +
          s"$ALICE in $TEST_GROUP_ANALYSTS, got: $version")
    }
  }

  @Test
  def userDefaultWinsOverGroupDefault(): Unit = {
    confController.withOverlay(Map(
      HADOOP_USER_GROUP_STATIC_MAPPING -> s"$ALICE=$TEST_GROUP_ANALYSTS",
      groupDefaultProfileKey(TEST_GROUP_ANALYSTS) -> EngineProfile.SPARK4.profileName,
      userDefaultProfileKey(ALICE) -> EngineProfile.SPARK3.profileName)) {
      val version = sparkVersion(Map.empty, user = ALICE)
      assertTrue(
        version.startsWith("3."),
        s"Expected user-default ${EngineProfile.SPARK3.profileName} to win over " +
          s"group-default ${EngineProfile.SPARK4.profileName}, got: $version")
    }
  }

  private def sparkVersion(
      sessionConf: Map[String, String],
      user: String = "anonymous"): String = {
    val rows = kyuubiService.query(sessionConf, "SELECT version()", user)
    if (rows.isEmpty) {
      fail("SELECT version() returned no rows")
    }
    String.valueOf(rows.head.values.head)
  }
}
