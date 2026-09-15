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

package org.apache.kyuubi.system.tests.model

sealed abstract class Component(
    val serviceName: String,
    val port: Int,
    val waitForHealthcheck: Boolean)

object Component {
  case object KYUUBI extends Component("kyuubi", 10009, true)
  case object KYUUBI_REST extends Component("kyuubi", 10099, false)
  case object POSTGRES_APP extends Component("postgres-app", 5432, false)
  case object TRINO extends Component("trino", 8080, false)

  val values: Seq[Component] = Seq(KYUUBI, KYUUBI_REST, POSTGRES_APP, TRINO)
}
