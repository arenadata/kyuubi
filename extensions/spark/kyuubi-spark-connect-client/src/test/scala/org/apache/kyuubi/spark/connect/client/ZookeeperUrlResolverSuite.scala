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

package org.apache.kyuubi.spark.connect.client

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.shaded.curator.framework.CuratorFrameworkFactory
import org.apache.kyuubi.shaded.curator.retry.ExponentialBackoffRetry
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

class ZookeeperUrlResolverSuite extends KyuubiFunSuite {

  private val zkServer = new EmbeddedZookeeper
  private var connectString: String = _

  override def beforeAll(): Unit = {
    val dataDir = Files.createTempDirectory("kyuubi-zk-resolver-test").toFile
    val conf = new KyuubiConf(false)
      .set(ZookeeperConf.ZK_DATA_DIR, dataDir.getAbsolutePath)
      .set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    zkServer.initialize(conf)
    zkServer.start()
    connectString = zkServer.getConnectString
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    zkServer.stop()
    super.afterAll()
  }

  // URL parsing tests - no ZK needed

  test("non-sc:// URL is returned unchanged") {
    val url = "jdbc:hive2://host:10009"
    assert(ZookeeperUrlResolver.resolve(url) == url)
  }

  test("sc:// URL without serviceDiscoveryMode=zooKeeper is returned unchanged") {
    val url = "sc://host:10199/;use_ssl=true"
    assert(ZookeeperUrlResolver.resolve(url) == url)
  }

  test("sc:// URL with other serviceDiscoveryMode is returned unchanged") {
    val url = "sc://host:10199/;serviceDiscoveryMode=etcd"
    assert(ZookeeperUrlResolver.resolve(url) == url)
  }

  test("sc:// URL with no path component is returned unchanged") {
    val url = "sc://host:10199"
    assert(ZookeeperUrlResolver.resolve(url) == url)
  }

  // ZK-based tests

  test("resolves ZK URL to registered server") {
    val namespace = "kyuubi_sc_resolve"
    registerServer(s"/$namespace", "host1:10199", "n1")
    val resolved = ZookeeperUrlResolver.resolve(zkUrl(namespace))
    assert(resolved == "sc://host1:10199")
  }

  test("preserves non-ZK params in resolved URL") {
    val namespace = "kyuubi_sc_params"
    registerServer(s"/$namespace", "host2:10199", "n1")
    val resolved = ZookeeperUrlResolver.resolve(zkUrl(namespace, extraParams = ";use_ssl=true"))
    assert(resolved.startsWith("sc://host2:10199"))
    assert(resolved.contains("use_ssl=true"))
    assert(!resolved.contains("serviceDiscoveryMode"))
    assert(!resolved.contains("zooKeeperNamespace"))
  }

  test("excludes specified server from candidates") {
    val namespace = "kyuubi_sc_exclude"
    registerServer(s"/$namespace", "hostA:10199", "n1")
    registerServer(s"/$namespace", "hostB:10199", "n2")
    val resolved =
      ZookeeperUrlResolver.resolve(zkUrl(namespace), excludeServers = Set("hostA:10199"))
    assert(resolved == "sc://hostB:10199")
  }

  test("throws RuntimeException when all candidates are excluded") {
    val namespace = "kyuubi_sc_allexcluded"
    registerServer(s"/$namespace", "host3:10199", "n1")
    intercept[RuntimeException] {
      ZookeeperUrlResolver.resolve(zkUrl(namespace), excludeServers = Set("host3:10199"))
    }
  }

  test("throws RuntimeException when namespace has no registered servers") {
    val namespace = "kyuubi_sc_empty"
    createEmptyNamespace(s"/$namespace")
    intercept[RuntimeException] {
      ZookeeperUrlResolver.resolve(zkUrl(namespace))
    }
  }

  private def zkUrl(namespace: String, extraParams: String = ""): String =
    s"sc://$connectString/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=$namespace$extraParams"

  private def createEmptyNamespace(zkPath: String): Unit = {
    val client = CuratorFrameworkFactory.newClient(
      connectString,
      new ExponentialBackoffRetry(1000, 3))
    client.start()
    try {
      if (client.checkExists().forPath(zkPath) == null) {
        client.create().creatingParentsIfNeeded().forPath(zkPath)
      }
    } finally {
      client.close()
    }
  }

  private def registerServer(zkPath: String, serverUrl: String, nodeName: String): Unit = {
    val client = CuratorFrameworkFactory.newClient(
      connectString,
      new ExponentialBackoffRetry(1000, 3))
    client.start()
    try {
      if (client.checkExists().forPath(zkPath) == null) {
        client.create().creatingParentsIfNeeded().forPath(zkPath)
      }
      val nodePath = s"$zkPath/$nodeName"
      if (client.checkExists().forPath(nodePath) != null) {
        client.delete().forPath(nodePath)
      }
      client.create().forPath(nodePath, serverUrl.getBytes(StandardCharsets.UTF_8))
    } finally {
      client.close()
    }
  }
}
