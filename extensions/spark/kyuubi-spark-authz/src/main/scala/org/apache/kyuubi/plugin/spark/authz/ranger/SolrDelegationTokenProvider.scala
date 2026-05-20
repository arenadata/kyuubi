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

import java.util.Properties

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.ranger.audit.utils.InMemoryJAASConfiguration
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpClientUtil, HttpSolrClient, Krb5HttpClientBuilder}
import org.apache.solr.client.solrj.request.DelegationTokenRequest
import org.apache.solr.hadoop.SolrDelegationTokenUtil
import org.apache.spark.SparkConf
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.util.DTokenUtils

class SolrDelegationTokenProvider extends HadoopDelegationTokenProvider {

  private val LOG = LoggerFactory.getLogger(getClass)

  private val AUDIT_DEST_PREFIX = "xasecure.audit.destination.solr"
  private val PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config"

  override def serviceName: String = "solr"

  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled && {
      try {
        val conf = SparkRangerAdminPlugin.getRangerConf
        val enabled = conf.get(AUDIT_DEST_PREFIX)
        enabled != null &&
          (enabled.equalsIgnoreCase("true") || enabled.equalsIgnoreCase("enabled"))
      } catch {
        case NonFatal(_) => false
      }
    }
  }

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      val rangerConf = SparkRangerAdminPlugin.getRangerConf

      val urls = rangerConf.get(AUDIT_DEST_PREFIX + ".urls")
      val zkHosts = rangerConf.get(AUDIT_DEST_PREFIX + ".zookeepers")

      val hasUrls = urls != null && urls.trim.nonEmpty && !urls.equalsIgnoreCase("NONE")
      val hasZk = zkHosts != null && zkHosts.trim.nonEmpty && !zkHosts.equalsIgnoreCase("NONE")

      if (!hasUrls && !hasZk) {
        LOG.warn("Cannot obtain Solr delegation token: " +
          s"neither $AUDIT_DEST_PREFIX.urls nor $AUDIT_DEST_PREFIX.zookeepers " +
          "is configured in Ranger audit config")
        return None
      }

      val renewer = DTokenUtils.getTokenRenewer(sparkConf, hadoopConf)
      LOG.info(s"Obtaining Solr delegation token, urls=$urls, zkHosts=$zkHosts, renewer=$renewer")

      // Set up JAAS from Ranger audit config (same as SolrAuditDestination.init())
      val props = new Properties()
      rangerConf.iterator().asScala.foreach(e => props.setProperty(e.getKey, e.getValue))

      if (System.getProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG) == null) {
        val forceInMemory = rangerConf.getBoolean(
          AUDIT_DEST_PREFIX + ".force.use.inmemory.jaas.config", false)
        if (forceInMemory) {
          System.setProperty(PROP_JAVA_SECURITY_AUTH_LOGIN_CONFIG, "/dev/null")
        }
      }
      InMemoryJAASConfiguration.init(props)

      val krbBuild = new Krb5HttpClientBuilder()
      HttpClientUtil.setHttpClientBuilder(krbBuild.getBuilder)

      val client = if (hasZk) {
        val zkHostsList = java.util.Arrays.asList(zkHosts.split(",").map(_.trim): _*)
        new CloudSolrClient.Builder(zkHostsList, java.util.Optional.empty[String]()).build()
      } else {
        new HttpSolrClient.Builder(urls.split(",")(0).trim).build()
      }

      try {
        val request = new DelegationTokenRequest.Get(renewer)
        val response = request.process(client)
        val dtString = response.getDelegationToken

        if (dtString == null) {
          LOG.warn("Solr returned null delegation token")
          return None
        }

        val serviceUrl = if (hasUrls) urls.split(",")(0).trim else zkHosts
        val token = SolrDelegationTokenUtil.createHadoopToken(dtString, serviceUrl)
        creds.addToken(token.getService, token)

        LOG.info("Obtained Solr delegation token successfully")
        None
      } finally {
        client.close()
      }
    } catch {
      case NonFatal(e) =>
        LOG.warn("Failed to obtain Solr delegation token. " +
          "If Solr audit is not used, set spark.security.credentials.solr.enabled=false", e)
        None
    }
  }
}
