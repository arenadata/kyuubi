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

package org.apache.kyuubi.gateway.it

import org.apache.kyuubi.engine.trino.TrinoQueryTests
import org.apache.kyuubi.gateway.tags.ContainerTest

/**
 * The Trino engine's own query suite, run through the gateway over binary HS2.
 *
 *   JDBC client -> gateway -> Trino
 *
 * Reused rather than rewritten, and that is the point: the gateway's claim is
 * that routing changes where a query goes and nothing about what it returns.
 * The suite that decides what Trino behaviour means is the one the engine is
 * already held to, so running it unchanged is what makes the claim checkable.
 */
@ContainerTest
class GatewayTrinoBinarySuite extends TrinoQueryTests with WithGatewayAndTrinoContainer {

  override protected def jdbcUrl: String = binaryUrl
}
