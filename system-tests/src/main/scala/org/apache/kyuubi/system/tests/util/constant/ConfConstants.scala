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

object ConfConstants {
  // keys
  val ENGINE_PROFILE = "kyuubi.engine.profile"
  val ENGINE_TYPE = "kyuubi.engine.type"
  val ENGINE_SHARE_LEVEL = "kyuubi.engine.share.level"
  val PROFILES_UNKNOWN_STRATEGY = "kyuubi.engine.profiles.unknown.strategy"
  val SESSION_CONF_IGNORE_LIST = "kyuubi.session.conf.ignore.list"
  val SESSION_CONF_RESTRICT_LIST = "kyuubi.session.conf.restrict.list"
  val TRINO_CONNECTION_CATALOG = "kyuubi.session.engine.trino.connection.catalog"
  val SPARK_APP_NAME = "spark.app.name"
  val SPARK_MASTER = "spark.master"

  // values used across ITs
  val SHARE_LEVEL_USER = "USER"
  val ENGINE_TYPE_SPARK_SQL = "SPARK_SQL"
  val UNKNOWN_STRATEGY_LOG = "LOG"
  val UNKNOWN_PROFILE = "does-not-exist"
  val HADOOP_USER_GROUP_STATIC_MAPPING = "hadoop.user.group.static.mapping.overrides"
  val TEST_GROUP_ANALYSTS = "analysts"

  def userDefaultProfileKey(user: String): String =
    s"___${user}___.$ENGINE_PROFILE"

  def groupDefaultProfileKey(group: String): String =
    s"___${group}___.$ENGINE_PROFILE"
}
