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
package org.apache.kyuubi.engine.jdbc.impala

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_JDBC_IMPALA_IMPERSONATION_ENABLED
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.jdbc.connection.JdbcConnectionProvider

class ImpalaConnectionProvider extends JdbcConnectionProvider {

  override val name: String = classOf[ImpalaConnectionProvider].getName

  override val driverClass: String = ImpalaConnectionProvider.driverClass

  /**
   * With `kyuubi.engine.jdbc.impala.impersonation.enabled`, run the Impala session as the
   * Kyuubi session user: the engine connects with its own credentials and Impala delegates to
   * `impala.doas.user` (requires `--authorized_proxy_user_config` on impalad).
   */
  override protected def connectionProperties(kyuubiConf: KyuubiConf): Map[String, String] =
    ImpalaConnectionProvider.delegationProperties(kyuubiConf)
}

object ImpalaConnectionProvider {
  // we should use kyuubi hive driver instead of original hive one in order
  // to get fixed getMoreResults()
  val driverClass: String = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"

  /** Impala delegation key, sent by the Kyuubi Hive driver as a raw OpenSession config. */
  val IMPALA_DOAS_USER = "impala.doas.user"

  def delegationProperties(kyuubiConf: KyuubiConf): Map[String, String] = {
    if (kyuubiConf.get(ENGINE_JDBC_IMPALA_IMPERSONATION_ENABLED)) {
      kyuubiConf.getOption(KYUUBI_SESSION_USER_KEY)
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(user => IMPALA_DOAS_USER -> user)
        .toMap
    } else {
      Map.empty
    }
  }
}
