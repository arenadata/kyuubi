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

package org.apache.kyuubi.server.flight

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys._

class KyuubiFlightSessionConfMiddlewareSuite extends KyuubiFunSuite {

  import KyuubiFlightSessionConfMiddleware.sessionConf

  test("kyuubi.* headers become session conf, other headers are ignored") {
    val conf = sessionConf(Seq(
      "kyuubi.engine.profile" -> "impala",
      "kyuubi.engine.share.level" -> " USER ",
      "x-user-name" -> "alice",
      "authorization" -> "Bearer abc",
      "spark.executor.memory" -> "4g",
      "user-agent" -> "grpc-go/1.0"))
    assert(conf === Map(
      "kyuubi.engine.profile" -> "impala",
      "kyuubi.engine.share.level" -> "USER"))
  }

  test("lower-cased header names map back to registered camelCase keys") {
    // gRPC lower-cases metadata keys on the wire
    val key = KyuubiConf.ENGINE_JDBC_CONNECTION_PROPAGATECREDENTIAL.key
    assert(key.exists(_.isUpper), "test needs a camelCase key")
    assert(sessionConf(Seq(key.toLowerCase -> "true")) === Map(key -> "true"))
    // unknown keys are kept as sent
    assert(sessionConf(Seq("kyuubi.some.unknown.key" -> "v")) ===
      Map("kyuubi.some.unknown.key" -> "v"))
  }

  test("identity and internal keys cannot be set by the client") {
    val reserved = Seq(
      KYUUBI_SESSION_REAL_USER_KEY,
      KYUUBI_SESSION_USER_KEY,
      KYUUBI_CLIENT_IP_KEY,
      KYUUBI_SERVER_IP_KEY,
      KYUUBI_SESSION_CONNECTION_URL_KEY,
      KYUUBI_ENGINE_CREDENTIALS_KEY,
      KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET,
      KyuubiConf.PROXY_USER.key,
      KyuubiConf.FRONTEND_PROTOCOLS.key)
    // as sent and lower-cased (the way they arrive over gRPC)
    val headers = reserved.map(_ -> "evil") ++ reserved.map(_.toLowerCase -> "evil")
    assert(sessionConf(headers :+ ("kyuubi.engine.profile" -> "impala")) ===
      Map("kyuubi.engine.profile" -> "impala"))
  }
}
