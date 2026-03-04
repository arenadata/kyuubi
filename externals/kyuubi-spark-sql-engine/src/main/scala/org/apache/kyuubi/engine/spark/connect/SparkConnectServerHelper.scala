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

import java.net.ServerSocket

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkConnectServerHelper {

  private val SERVICE_CLASS = "org.apache.spark.sql.connect.service.SparkConnectService$"

  private def serviceModule(): AnyRef = {
    val cls = Class.forName(SERVICE_CLASS)
    cls.getField("MODULE$").get(null)
  }

  /**
   * Starts the Spark Connect gRPC server inside the engine JVM.
   * Returns the port the server actually bound to.
   * If requestedPort is 0, an available port is pre-allocated and used.
   *
   * Requires spark-connect jar on the classpath (e.g. copy
   * spark-connect_*.jar into $SPARK_HOME/jars/).
   */

  def start(spark: SparkSession, requestedPort: Int): Int = {
    val port = if (requestedPort == 0) findFreePort() else requestedPort
    // TODO: set port before spark context creating
    spark.sparkContext.getConf.set("spark.connect.grpc.binding.port", port.toString)
    try {
      val cls = Class.forName(SERVICE_CLASS)
      val module = cls.getField("MODULE$").get(null)
      cls.getMethod("start", classOf[SparkContext]).invoke(module, spark.sparkContext)
      // Read the actual port SparkConnectService bound to
      val localPortMethod = cls.getDeclaredMethod("localPort")
      localPortMethod.setAccessible(true)
      localPortMethod.invoke(module).asInstanceOf[Int]
    } catch {
      case e: ClassNotFoundException =>
        throw new RuntimeException(
          s"spark-connect is not available on the classpath. " +
            s"Copy spark-connect_*.jar into $$SPARK_HOME/jars/ " +
            s"or add it via spark.driver.extraClassPath.",
          e)
    }
  }

  def stop(): Unit = {
    try {
      val module = serviceModule()
      module.getClass.getMethod("stop").invoke(module)
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError =>
      // spark-connect was not started, nothing to stop
    }
  }

  private def findFreePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }
}
