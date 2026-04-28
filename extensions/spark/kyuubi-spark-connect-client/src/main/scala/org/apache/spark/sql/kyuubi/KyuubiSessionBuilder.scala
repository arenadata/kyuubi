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

import org.apache.kyuubi.spark.connect.client.KyuubiTokenClient

/**
 * Builder for a Kyuubi-authenticated Spark Connect session.
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

  private val clientBuilder = SparkConnectClient.builder().connectionString(url)

  private val tokenClient: Option[KyuubiTokenClient] = auth match {
    case KyuubiAuthType.NONE => None
    case _ =>
      val client = new KyuubiTokenClient(
        clientBuilder.host,
        clientBuilder.port,
        clientBuilder.sslEnabled)
      client.getToken(auth, username, password)
      clientBuilder.option("authorization", s"Bearer ${client.currentToken}")
      Some(client)
  }

  def getOrCreate(): SparkSession =
    SparkSession.builder().client(clientBuilder.build()).getOrCreate()

  def renew(): Unit = tokenClient.foreach(_.renewToken())

  def revoke(): Unit = tokenClient.foreach(_.revokeToken())
}
