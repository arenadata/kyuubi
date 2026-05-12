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

package org.apache.spark.sql.connect

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service.SparkConnectService

/**
 * Helper object to start/stop Spark Connect gRPC server inside the engine JVM.
 * Lives in `org.apache.spark.sql.connect` to access package-private members of
 * [[SparkConnectService]].
 */
object SparkConnectServerHelper {

  /**
   * Starts the Spark Connect gRPC server and returns the bound port.
   */
  def start(spark: SparkSession): Int = {
    SparkConnectService.start(spark.sparkContext)
    SparkConnectService.localPort
  }

  /**
   * Stops the Spark Connect gRPC server.
   */
  def stop(): Unit = {
    SparkConnectService.stop()
  }
}
