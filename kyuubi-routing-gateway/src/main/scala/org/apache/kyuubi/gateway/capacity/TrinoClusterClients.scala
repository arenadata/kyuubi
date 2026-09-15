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

package org.apache.kyuubi.gateway.capacity

import java.net.URI
import java.util.concurrent.ConcurrentHashMap

import okhttp3.OkHttpClient

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.session.TrinoSessionImpl
import org.apache.kyuubi.gateway.cluster.ClusterRef

/**
 * HTTP clients for reaching a cluster's coordinator or workers on the
 * gateway's own behalf.
 *
 * Built the way a session routed to the same cluster builds its own - the
 * gateway's configuration with the cluster's session settings on top, through
 * the same builder - so a cluster that needs Kerberos, a password or TLS gets
 * them here too. A bare client would be refused by exactly the clusters that
 * are secured, and would look like an unreachable coordinator.
 *
 * One client per address and settings, kept across passes: a Kerberos client
 * holds its login, and rebuilding it every few seconds would log in again each
 * time.
 */
class TrinoClusterClients(conf: KyuubiConf, user: String) extends (ClusterRef => OkHttpClient) {

  private val clients = new ConcurrentHashMap[(String, Map[String, String]), OkHttpClient]()

  override def apply(cluster: ClusterRef): OkHttpClient =
    clients.computeIfAbsent(
      (cluster.url, cluster.sessionConf),
      _ => {
        val effective = conf.clone
        cluster.sessionConf.foreach { case (k, v) => effective.set(k, v) }
        TrinoSessionImpl.createHttpClient(effective, URI.create(cluster.url).getScheme, user)
      })
}
