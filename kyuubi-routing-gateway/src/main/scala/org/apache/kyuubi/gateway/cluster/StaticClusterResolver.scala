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

package org.apache.kyuubi.gateway.cluster

import org.apache.kyuubi.Logging

/**
 * Resolver backed by static configuration. Intended as the first step and as a
 * fallback for backends the Kubernetes resolver does not manage.
 *
 * Clusters are declared as:
 * {{{
 *   kyuubi.gateway.cluster.<name>.url     = http://trino-team-a:8080
 *   kyuubi.gateway.cluster.<name>.engine  = trino
 *   kyuubi.gateway.cluster.<name>.users   = alice,bob
 *   kyuubi.gateway.cluster.<name>.default = true
 * }}}
 *
 * Any other `kyuubi.gateway.cluster.<name>.session.<k> = v` pair becomes session
 * configuration `<k> = v` for sessions routed to that cluster.
 */
class StaticClusterResolver(conf: Map[String, String]) extends ClusterResolver with Logging {

  import StaticClusterResolver._

  private val parsed: Seq[ParsedCluster] = parse(conf)

  override val clusters: Seq[ClusterRef] = parsed.map(_.ref)

  override def resolve(user: String, sessionConf: Map[String, String]): Option[ClusterRef] = {
    val explicit = parsed.find(_.users.contains(user))
    val fallback = parsed.find(_.isDefault)
    val chosen = explicit.orElse(fallback)
    chosen match {
      case Some(c) => debug(s"Routing user $user to cluster ${c.ref.name}")
      case None => warn(s"No cluster allowed for user $user")
    }
    chosen.map(_.ref)
  }
}

object StaticClusterResolver {

  val PREFIX = "kyuubi.gateway.cluster."

  private case class ParsedCluster(ref: ClusterRef, users: Set[String], isDefault: Boolean)

  private def parse(conf: Map[String, String]): Seq[ParsedCluster] = {
    val names = conf.keys.filter(_.startsWith(PREFIX)).flatMap { key =>
      val rest = key.substring(PREFIX.length)
      val dot = rest.indexOf('.')
      if (dot > 0) Some(rest.substring(0, dot)) else None
    }.toSeq.distinct.sorted

    names.flatMap { name =>
      val base = PREFIX + name + "."
      conf.get(base + "url").map { url =>
        val sessionPrefix = base + "session."
        val sessionConf = conf.collect {
          case (k, v) if k.startsWith(sessionPrefix) => k.substring(sessionPrefix.length) -> v
        }
        ParsedCluster(
          ClusterRef(
            name = name,
            engine = conf.getOrElse(base + "engine", "trino"),
            url = url,
            sessionConf = sessionConf),
          users = conf.get(base + "users")
            .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet)
            .getOrElse(Set.empty),
          isDefault = conf.get(base + "default").exists(_.toBoolean))
      }
    }
  }
}
