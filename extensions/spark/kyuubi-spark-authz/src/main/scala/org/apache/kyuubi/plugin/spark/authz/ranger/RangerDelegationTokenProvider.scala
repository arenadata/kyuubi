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

import scala.util.control.NonFatal
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.kyuubi.plugin.spark.authz.util.DTokenUtils
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.spark.SparkConf
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.slf4j.LoggerFactory

class RangerDelegationTokenProvider extends HadoopDelegationTokenProvider {

  private val LOG = LoggerFactory.getLogger(getClass)

  private val PROPERTY_PREFIX = "ranger.plugin.spark"

  private lazy val adminClient = {
    val conf = SparkRangerAdminPlugin.getRangerConf
    val serviceName = conf.get(PROPERTY_PREFIX + ".service.name", "spark")
    val client = new RangerAdminRESTClient()
    client.init(serviceName, "sparkSubmit", PROPERTY_PREFIX, conf)
    client
  }

  override def serviceName: String = "ranger"

  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      val rangerUrl = SparkRangerAdminPlugin.getRangerConf
        .get(PROPERTY_PREFIX + ".policy.rest.url")

      if (rangerUrl == null || rangerUrl.isEmpty) {
        LOG.warn("Cannot obtain Ranger delegation token: " +
          PROPERTY_PREFIX + ".policy.rest.url is not configured")
        return None
      }

      val renewer = DTokenUtils.getTokenRenewer(sparkConf, hadoopConf)

      LOG.info(s"Obtaining Ranger delegation token from $rangerUrl, renewer=$renewer")

      val token = adminClient.getDelegationToken(renewer)

      creds.addToken(token.getService, token)

      val ident = token.decodeIdentifier()
      LOG.info(s"Obtained Ranger delegation token, " +
        s"owner=${ident.getUser}, renewer=$renewer, maxDate=${ident.getMaxDate}")

      None
    } catch {
      case NonFatal(e) =>
        LOG.warn(s"Failed to obtain Ranger delegation token. " +
          s"If Ranger is not used, set spark.security.credentials.ranger.enabled=false", e)
        None
    }
  }

}
