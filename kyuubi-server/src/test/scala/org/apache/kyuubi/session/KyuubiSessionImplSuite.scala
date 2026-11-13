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
package org.apache.kyuubi.session

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf.ENGINE_JDBC_IMPALA_IMPERSONATION_ENABLED

class KyuubiSessionImplSuite extends KyuubiFunSuite {

  import KyuubiSessionImpl.partitionServerOnly

  private val key = ENGINE_JDBC_IMPALA_IMPERSONATION_ENABLED.key

  test("clients cannot set the Impala impersonation flag") {
    val (dropped, kept) = partitionServerOnly(Map(
      key -> "false",
      "kyuubi.engine.profile" -> "impala",
      "kyuubi.engine.share.level" -> "USER"))
    assert(dropped === Map(key -> "false"))
    assert(kept === Map("kyuubi.engine.profile" -> "impala", "kyuubi.engine.share.level" -> "USER"))
  }

  test("server-only keys are matched ignoring case and surrounding spaces") {
    val (dropped, kept) = partitionServerOnly(Map(key.toUpperCase -> "false", s" $key " -> "no"))
    assert(dropped.size === 2)
    assert(kept.isEmpty)
  }

  test("a client configuration without server-only keys is kept as is") {
    val conf = Map("kyuubi.engine.profile" -> "impala", "kyuubi.engine.jdbc.fetch.size" -> "100")
    assert(partitionServerOnly(conf) === (Map.empty, conf))
  }
}
