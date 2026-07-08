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
import java.util.Random

import scala.collection.JavaConverters._

import org.apache.kyuubi.shaded.curator.framework.CuratorFrameworkFactory
import org.apache.kyuubi.shaded.curator.retry.ExponentialBackoffRetry
import org.apache.kyuubi.shaded.zookeeper.KeeperException

class NoServersAvailableException(message: String) extends RuntimeException(message)

/**
 * Resolves ZooKeeper-based Spark Connect URL to a direct sc://host:port URL.
 *
 * Accepts URLs of the form:
 *   sc://zk1:2181,zk2:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=ns[;other=params]
 *
 * Connects to ZooKeeper, lists live Kyuubi Spark Connect servers registered under
 * given namespace, picks one at random, and returns sc://host:port[/;other=params].
 * Non-ZK parameters (e.g. use_ssl) are preserved in the resolved URL.
 * Returns URL unchanged if serviceDiscoveryMode is not zooKeeper.
 */
object ZookeeperUrlResolver {

  private val random = new Random()

  def resolve(url: String, excludeServers: Set[String] = Set.empty): String = {
    if (!url.startsWith("sc://")) return url

    val rest = url.stripPrefix("sc://")
    val slashIdx = rest.indexOf('/')
    if (slashIdx == -1) return url

    val zkAddresses = rest.substring(0, slashIdx)
    val paramsPart = rest.substring(slashIdx + 1).stripPrefix(";")

    val params = paramsPart.split(";").collect {
      case p if p.contains("=") =>
        val eq = p.indexOf('=')
        p.substring(0, eq) -> p.substring(eq + 1)
    }.toMap

    if (params.getOrElse("serviceDiscoveryMode", "") != "zooKeeper") return url

    val namespace = params.getOrElse("zooKeeperNamespace", "kyuubi_sc")
    val zkPath = "/" + namespace
    val connectUrl = resolveFromZooKeeper(zkAddresses, zkPath, excludeServers)

    val nonZkParams = params.filterKeys(k =>
      k != "serviceDiscoveryMode" && k != "zooKeeperNamespace")
    val resolved = s"sc://$connectUrl"
    if (nonZkParams.isEmpty) resolved
    else resolved + "/;" + nonZkParams.map { case (k, v) => s"$k=$v" }.mkString(";")
  }

  private def resolveFromZooKeeper(
      zkAddresses: String,
      zkPath: String,
      excludeServers: Set[String]): String = {
    val client = CuratorFrameworkFactory.newClient(
      zkAddresses,
      new ExponentialBackoffRetry(1000, 3))
    client.start()
    try {
      val children = try {
        client.getChildren.forPath(zkPath).asScala
      } catch {
        case e: KeeperException =>
          throw new NoServersAvailableException(
            s"ZooKeeper path $zkPath does not exist or is inaccessible: ${e.getMessage}")
      }
      val candidates = children
        .map(node => new String(client.getData.forPath(s"$zkPath/$node"), StandardCharsets.UTF_8))
        .filterNot(excludeServers.contains)
      if (candidates.isEmpty) {
        throw new NoServersAvailableException(
          s"No Kyuubi Spark Connect servers found in ZooKeeper at $zkPath" +
            (if (excludeServers.nonEmpty) s" (excluded: ${excludeServers.mkString(", ")})" else ""))
      }
      candidates(random.nextInt(candidates.size))
    } finally {
      client.close()
    }
  }
}
