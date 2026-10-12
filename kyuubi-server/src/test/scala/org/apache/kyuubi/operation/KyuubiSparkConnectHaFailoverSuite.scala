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

package org.apache.kyuubi.operation

import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.kyuubi.KyuubiSessionBuilder

import org.apache.kyuubi.WithSparkConnectHaCluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_NAMESPACE, HA_SPARK_CONNECT_NAMESPACE}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient

class KyuubiSparkConnectHaFailoverSuite extends WithSparkConnectHaCluster {

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(ENGINE_SHARE_LEVEL, "connection")
    .set(HA_SPARK_CONNECT_NAMESPACE, "kyuubi_sc_ha_it")
    .set(HA_NAMESPACE, "kyuubi_thrift_ha_it")

  private def withSpark(body: SparkSession => Unit): Unit = {
    val spark = new KyuubiSessionBuilder(zkConnectUrl).getOrCreate()
    try {
      body(spark)
    } finally {
      spark.stop()
    }
  }

  test("SC servers register under HA_SPARK_CONNECT_NAMESPACE only") {
    withDiscoveryClient(conf) { discoveryClient =>
      val scPath = s"/$sparkConnectNamespace"
      val thriftPath = s"/${conf.get(HA_NAMESPACE)}"
      assert(discoveryClient.pathExists(scPath))
      assert(discoveryClient.getChildren(scPath).size === serverCount)
      assert(
        discoveryClient.pathNonExists(thriftPath) ||
          discoveryClient.getChildren(thriftPath).isEmpty,
        s"Spark Connect endpoints must not register under Thrift namespace $thriftPath")
    }
  }

  test("failover after SC frontend crash resumes SQL on peer and reuses CONNECTION engine") {
    withSpark { spark =>
      spark.sql("CREATE OR REPLACE TEMP VIEW ha_reuse_view AS SELECT 41 AS v")
      assert(spark.sql("SELECT v FROM ha_reuse_view").collect()(0).getInt(0) === 41)

      val active = findServerWithSessions
      crashSparkConnectFrontend(active)

      eventually(timeout(60.seconds), interval(500.millis)) {
        assert(spark.sql("SELECT v FROM ha_reuse_view").collect()(0).getInt(0) === 41)
      }

      val peer = servers.find(_ ne active).get
      assert(
        peer.backendService.sessionManager.allSessions().nonEmpty,
        "peer server should open a session after failover")
    }
  }
}
