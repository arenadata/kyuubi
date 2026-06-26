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

package org.apache.spark.sql.kyuubi;

import org.apache.spark.sql.connect.client.SparkConnectClient;

/**
 * Bridges Scala-signature / JVM-bytecode mismatch.
 *
 * spark-connect-client-jvm jar is built with shading (io.grpc -> org.sparkproject.io.grpc) 
 * but its embedded Scala signature still references io.grpc.*.
 * The Scala compiler reads the Scala signature and types SparkConnectClient APIs as io.grpc.*,
 * while the JVM bytecode says org.sparkproject.io.grpc.*. This causes NoSuchMethodError at
 * runtime when Scala-compiled code calls createChannel() or the SparkConnectClient constructor.
 *
 * Java reads only JVM bytecode (not Scala signatures), so this class sees and uses the correct
 * org.sparkproject.io.grpc.* types and compiles without any mismatch.
 */
class SparkConnectBridge {

  static org.sparkproject.io.grpc.ManagedChannel createChannel(
      SparkConnectClient.Configuration config) {
    return config.createChannel();
  }

  static SparkConnectClient create(
      SparkConnectClient.Configuration config,
      org.sparkproject.io.grpc.ManagedChannel channel) {
    return new SparkConnectClient(config, channel);
  }
}
