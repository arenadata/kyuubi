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

package org.apache.kyuubi

import java.util.concurrent.TimeUnit

import io.grpc.Server

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_SPARK_CONNECT_NAMESPACE
import org.apache.kyuubi.server.{KyuubiServer, KyuubiSparkConnectFrontendService}
import org.apache.kyuubi.util.reflect.ReflectUtils._

trait WithSparkConnectHaCluster extends WithKyuubiHaCluster {

  override protected def serverNamePrefix: String = "kyuubi-sc-ha"

  override protected def configureFrontends(conf: KyuubiConf): Unit = {
    conf.set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.SPARK_CONNECT.toString))
    conf.set(FRONTEND_SPARK_CONNECT_BIND_PORT, 0)
    conf.setIfMissing(FRONTEND_SPARK_CONNECT_BIND_HOST.key, "localhost")
  }

  protected def sparkConnectNamespace: String = conf.get(HA_SPARK_CONNECT_NAMESPACE)

  protected def zkConnectUrl: String =
    s"sc://$zkAddresses/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=$sparkConnectNamespace"

  protected def connectUrl(server: KyuubiServer): String =
    server.frontendServices.head.connectionUrl

  protected def crashSparkConnectFrontend(server: KyuubiServer): Unit = {
    val fe = server.frontendServices.head.asInstanceOf[KyuubiSparkConnectFrontendService]
    val grpcServer = getField[Server](fe, "grpcServer")
    grpcServer.shutdownNow()
    grpcServer.awaitTermination(10, TimeUnit.SECONDS)
  }
}
