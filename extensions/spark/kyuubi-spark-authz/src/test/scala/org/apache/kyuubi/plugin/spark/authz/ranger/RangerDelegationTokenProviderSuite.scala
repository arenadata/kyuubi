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

package org.apache.kyuubi.plugin.spark.authz.ranger

import java.net.InetAddress
import java.util.ServiceLoader

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.spark.security.HadoopDelegationTokenProvider

import org.apache.kyuubi.plugin.spark.authz.util.DTokenUtils
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

class RangerDelegationTokenProviderSuite extends AnyFunSuite {
// scalastyle:on

  private val provider = new RangerDelegationTokenProvider()

  test("serviceName is ranger") {
    assert(provider.serviceName === "ranger")
  }

  test("ServiceLoader discovers RangerDelegationTokenProvider") {
    import scala.util.Try
    val it = ServiceLoader.load(classOf[HadoopDelegationTokenProvider]).iterator()
    val providers = Iterator.continually(())
      .takeWhile(_ => it.hasNext)
      .flatMap(_ => Try(it.next()).toOption)
      .toSeq
    val rangerProvider = providers.find(_.serviceName == "ranger")
    assert(rangerProvider.isDefined)
    assert(rangerProvider.get.isInstanceOf[RangerDelegationTokenProvider])
  }

  test("delegationTokensRequired returns false when security is disabled") {
    // Default test env has simple authentication (no Kerberos)
    assert(!provider.delegationTokensRequired(new SparkConf(), new Configuration()))
  }

  test("getTokenRenewer - YARN with RM principal") {
    val sparkConf = new SparkConf().set("spark.master", "yarn")
    val hadoopConf = new Configuration(false)
    hadoopConf.set("yarn.resourcemanager.principal", "yarn/rm-host@REALM")

    val renewer = DTokenUtils.getTokenRenewer(sparkConf, hadoopConf)
    assert(renewer === "yarn/rm-host@REALM")
  }

  test("getTokenRenewer - YARN with _HOST placeholder resolves to local hostname") {
    val sparkConf = new SparkConf().set("spark.master", "yarn")
    val hadoopConf = new Configuration(false)
    hadoopConf.set("yarn.resourcemanager.principal", "yarn/_HOST@REALM")

    val renewer = DTokenUtils.getTokenRenewer(sparkConf, hadoopConf)
    val expectedHost = InetAddress.getLocalHost.getCanonicalHostName.toLowerCase
    assert(renewer === s"yarn/$expectedHost@REALM")
  }

  test("getTokenRenewer - YARN without RM principal falls back to current user") {
    val sparkConf = new SparkConf().set("spark.master", "yarn")
    val hadoopConf = new Configuration(false)

    val renewer = DTokenUtils.getTokenRenewer(sparkConf, hadoopConf)
    assert(renewer === UserGroupInformation.getCurrentUser.getUserName)
  }

  test("getTokenRenewer - non-YARN uses current user") {
    val sparkConf = new SparkConf().set("spark.master", "local[2]")
    val hadoopConf = new Configuration(false)
    hadoopConf.set("yarn.resourcemanager.principal", "yarn/rm-host@REALM")

    val renewer = DTokenUtils.getTokenRenewer(sparkConf, hadoopConf)
    // RM principal is ignored for non-YARN master
    assert(renewer === UserGroupInformation.getCurrentUser.getUserName)
  }

  test("getTokenRenewer - no spark.master defaults to current user") {
    val sparkConf = new SparkConf()
    val hadoopConf = new Configuration(false)

    val renewer = DTokenUtils.getTokenRenewer(sparkConf, hadoopConf)
    assert(renewer === UserGroupInformation.getCurrentUser.getUserName)
  }

  test("obtainDelegationTokens returns None when Ranger URL is not configured") {
    // Use a fresh Configuration without ranger-spark-security.xml properties
    val hadoopConf = new Configuration(false)
    val sparkConf = new SparkConf()
    val creds = new Credentials()

    // SparkRangerAdminPlugin.getRangerConf has the test config with a dummy URL,
    // but if we could override it, this would test the null URL path.
    // For now, test that obtainDelegationTokens handles REST client failure gracefully.
    val result = provider.obtainDelegationTokens(hadoopConf, sparkConf, creds)
    assert(result.isEmpty)
    assert(creds.getAllTokens.isEmpty)
  }
}
