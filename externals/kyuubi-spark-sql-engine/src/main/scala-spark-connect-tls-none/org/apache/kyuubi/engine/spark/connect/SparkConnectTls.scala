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

package org.apache.kyuubi.engine.spark.connect

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SSL_ENABLED
import org.apache.kyuubi.util.EngineSslServerMaterial

/**
 * Spark Connect TLS of the engine, for Spark builds without Spark Connect TLS
 * (`spark.connect.tls.support=none`), such as the Apache Spark ones. Builds for Spark with
 * Spark Connect TLS compile the variant in `src/main/scala-spark-connect-tls-native` instead.
 */
object SparkConnectTls {

  /** Whether this engine build can serve Spark Connect with TLS. */
  val supported: Boolean = false

  /** Fails unless this engine build can serve Spark Connect with TLS. */
  def requireSupported(): Unit = throw unsupported()

  /** Fails, like [[requireSupported]]. */
  def checkPolicy(material: EngineSslServerMaterial): Unit = throw unsupported()

  /** Never starts Spark Connect, it would not serve TLS. */
  def start(spark: SparkSession, material: EngineSslServerMaterial): Int = throw unsupported()

  private def unsupported(): IllegalStateException = new IllegalStateException(
    s"${ENGINE_SSL_ENABLED.key} is true, but this engine cannot serve Spark Connect with TLS on " +
      s"Spark $SPARK_VERSION: it was built without Spark Connect TLS " +
      "(spark.connect.tls.support=none). Spark Connect TLS needs an ADH Spark build with Spark " +
      "Connect TLS and a Kyuubi engine built for it with spark.connect.tls.support=native.")
}
