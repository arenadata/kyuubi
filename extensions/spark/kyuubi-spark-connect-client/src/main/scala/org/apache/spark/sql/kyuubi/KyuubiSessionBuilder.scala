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

// Placed in org.apache.spark.sql.kyuubi to access private[sql] SparkConnectClient APIs.
package org.apache.spark.sql.kyuubi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.SparkConnectClient

import org.apache.kyuubi.spark.connect.client.{KyuubiTokenClient, ZookeeperUrlResolver}
import org.apache.kyuubi.util.reflect.{DynClasses, DynMethods}
import org.apache.kyuubi.util.reflect.ReflectUtils.invokeAs

/**
 * Builder for a Kyuubi-authenticated Spark Connect session with transparent ZooKeeper HA
 * failover.
 *
 * When the connected Kyuubi server becomes unavailable, [[FailoverManagedChannel]] silently
 * switches to the next live server and [[ExecutePlanResponseReattachableIterator]] resumes
 * the in-flight operation via ReattachExecute - no exception reaches user code.
 *
 * Usage:
 * {{{
 *   // no auth:
 *   val spark = new KyuubiSessionBuilder("sc://host:10199").getOrCreate()
 *
 *   // Kerberos (reads TGT from kinit ticket cache):
 *   val spark = new KyuubiSessionBuilder("sc://host:10199/;use_ssl=true",
 *     KyuubiAuthType.KERBEROS).getOrCreate()
 *
 *   // LDAP:
 *   val spark = new KyuubiSessionBuilder("sc://host:10199/;use_ssl=true",
 *     KyuubiAuthType.LDAP, "john", "secret").getOrCreate()
 *
 *   // ZooKeeper HA (Kerberos) - failover is fully transparent:
 *   val spark = new KyuubiSessionBuilder(
 *     "sc://zk1:2181,zk2:2181,zk3:2181/;serviceDiscoveryMode=zooKeeper" +
 *     ";zooKeeperNamespace=arenadata/cluster/4/kyuubi_sc;use_ssl=true",
 *     KyuubiAuthType.KERBEROS).getOrCreate()
 *
 *   spark.sql("SELECT current_user()").show()
 *   spark.stop()  // sends ReleaseSession, server revokes token automatically
 * }}}
 */
class KyuubiSessionBuilder(
    url: String,
    auth: KyuubiAuthType,
    username: String,
    password: String) {

  def this(url: String) = this(url, KyuubiAuthType.NONE, null, null)

  def this(url: String, auth: KyuubiAuthType) = this(url, auth, null, null)

  private val resolvedUrl = ZookeeperUrlResolver.resolve(url)
  private val configBuilder = SparkConnectClient.builder().connectionString(resolvedUrl)

  private val tokenClient: Option[KyuubiTokenClient] = auth match {
    case KyuubiAuthType.NONE => None
    case _ =>
      val client = new KyuubiTokenClient(
        configBuilder.host,
        configBuilder.port,
        configBuilder.sslEnabled)
      client.getToken(auth, username, password)
      Some(client)
  }

  private val failoverChannel = new FailoverManagedChannel(url, tokenClient)
  failoverChannel.init(resolvedUrl)

  private val sparkClient = SparkConnectBridge.create(configBuilder.configuration, failoverChannel)
  // private val sparkClient = new SparkConnectClient(configBuilder.configuration, failoverChannel)

  def getOrCreate(): SparkSession = {
    // SPARK-49282 (4.1.0): concrete Connect SparkSession + Builder.client(...) moved from
    // org.apache.spark.sql.SparkSession to org.apache.spark.sql.connect.SparkSession.
    val sessionClz = DynClasses.builder()
      .impl("org.apache.spark.sql.connect.SparkSession")
      .impl("org.apache.spark.sql.SparkSession")
      .build()

    // builder()/client()/getOrCreate() also need reflection: `client(SparkConnectClient)` is
    // declared directly on each concrete Builder class in both versions, never on a shared
    // supertype; `builder()`/`getOrCreate()` do have a shared `SparkSessionBuilder` supertype in
    // 4.2, but that type doesn't exist at all in 3.5.

    // spark-3.5:
    // org.apache.spark.sql.SparkSession.builder().client(client).getOrCreate()

    // spark-4.2:
    // org.apache.spark.sql.connect.SparkSession.builder().client(client).getOrCreate()

    val builder = invokeAs[AnyRef](sessionClz, "builder")
    val builderWithClient =
      invokeAs[AnyRef](builder, "client", classOf[SparkConnectClient] -> sparkClient)

    // getOrCreate() opens the connection, so it is the one call here that fails for real
    // reasons (Kerberos rejected, server unreachable). Resolved and invoked separately, via
    // invokeChecked, so that reason survives instead of becoming "does not have getOrCreate()".
    val getOrCreateMethod = DynMethods.builder("getOrCreate")
      .hiddenImpl(builderWithClient.getClass)
      .impl(builderWithClient.getClass)
      .buildChecked(builderWithClient)
    getOrCreateMethod.invokeChecked[SparkSession]()
  }

  def renew(): Unit = tokenClient.foreach(_.renewToken())

  def revoke(): Unit = tokenClient.foreach(_.revokeToken())
}
