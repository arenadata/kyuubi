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

package org.apache.kyuubi.system.tests.util.constant

import org.apache.kyuubi.system.tests.model.WaitParams

object TimeoutConstants {
  val CONTAINER_OPERATION_DELAY_MS: Long = 5000L
  val SHORT_WAIT_PARAMS: WaitParams = WaitParams(waitTimeoutMs = 1000, attempts = 10)
  val DEFAULT_WAIT_PARAMS: WaitParams = WaitParams(waitTimeoutMs = 1000, attempts = 30)
  val EXTENDED_WAIT_PARAMS: WaitParams = WaitParams(waitTimeoutMs = 1000, attempts = 90)
  val KYUUBI_RESTART_WAIT_PARAMS: WaitParams = WaitParams(waitTimeoutMs = 2000, attempts = 120)
}
