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

package org.apache.kyuubi.engine

import org.apache.kyuubi.config.KyuubiConf._

class EngineProfileJdbcLogSuite extends EngineProfileJdbcTest {
  import EngineProfileJdbcTest._

  override protected def unknownStrategy: String = "LOG"

  test("ADH-8475: JDBC LOG strategy ignores an unknown profile without choosing a lower default") {
    withServerConf(
      principalKey(PROFILE_USER, ENGINE_PROFILE.key) -> "beta",
      TYPE_DEFAULT -> "alpha") {
      assertProfile(None, Map(ENGINE_PROFILE.key -> "missing"))
    }
  }

  test("ADH-9238: JDBC LOG strategy still rejects explicitly blacklisted profiles") {
    withServerConf(principalKey(SECOND_GROUP, ENGINE_PROFILES_BLACKLIST.key) -> " alpha ") {
      assertBlacklisted("alpha")
    }
  }
}
