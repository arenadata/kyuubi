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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.kyuubi.KyuubiSessionBuilder

import org.apache.kyuubi.{Utils, WithSparkConnectServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ADDRESSES

class KyuubiSparkConnectOperationSuite extends WithSparkConnectServer {

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(ENGINE_SHARE_LEVEL, "server")

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .remote(s"sc://$connectUrl")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("SELECT 1") {
    val result = spark.sql("SELECT 1").collect()
    assert(result.length === 1)
    assert(result(0).getInt(0) === 1)
  }

  test("SELECT some expression") {
    val result = spark.sql("SELECT 1 + 5 AS val").collect()
    assert(result(0).getInt(0) === 6)
  }

  test("CREATE TEMP VIEW and SELECT") {
    spark.sql("CREATE OR REPLACE TEMP VIEW view1 AS SELECT 5 AS v1, 10 AS v2")
    val result = spark.sql("SELECT v1, v2 FROM view1").collect()
    assert(result(0).getInt(0) === 5)
    assert(result(0).getAs[Integer]("v2") === 10)
  }

  // HA: KyuubiSessionBuilder resolves ZK URL to connect to running server

  test("KyuubiSessionBuilder with ZK URL executes SQL via FailoverManagedChannel") {
    val zkAddresses = conf.get(HA_ADDRESSES)
    val namespace = conf.get(HighAvailabilityConf.HA_SPARK_CONNECT_NAMESPACE)
    val zkUrl = s"sc://$zkAddresses/;serviceDiscoveryMode=zooKeeper" +
      s";zooKeeperNamespace=$namespace"

    val sparkViaZk = new KyuubiSessionBuilder(zkUrl).getOrCreate()
    try {
      val result = sparkViaZk.sql("SELECT 42 AS answer").collect()
      assert(result.length === 1)
      assert(result(0).getInt(0) === 42)
    } finally {
      sparkViaZk.stop()
    }
  }

  test("KyuubiSessionBuilder ZK URL resolves to running Kyuubi server") {
    val zkAddresses = conf.get(HA_ADDRESSES)
    val namespace = conf.get(HighAvailabilityConf.HA_SPARK_CONNECT_NAMESPACE)
    val zkUrl = s"sc://$zkAddresses/;serviceDiscoveryMode=zooKeeper" +
      s";zooKeeperNamespace=$namespace"

    val sparkViaZk = new KyuubiSessionBuilder(zkUrl).getOrCreate()
    try {
      val result = sparkViaZk.sql("SELECT current_user()").collect()
      assert(result.length === 1)
      // No auth here, so session user is the OS user
      assert(result(0).getString(0) === Utils.currentUser)
    } finally {
      sparkViaZk.stop()
    }
  }
}
