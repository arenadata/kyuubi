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

/**
 * A backend cluster the gateway can route a session to.
 *
 * @param name     cluster identity, unique within the gateway
 * @param engine   engine type, currently only "trino"
 * @param url      address the engine client connects to
 * @param sessionConf extra session configuration applied to sessions routed here
 */
case class ClusterRef(
    name: String,
    engine: String,
    url: String,
    sessionConf: Map[String, String] = Map.empty)

/**
 * Resolves the cluster a session belongs to.
 *
 * Resolution happens once per session, at open time: the routing key is the
 * authenticated identity, which is constant for the life of the session.
 */
trait ClusterResolver {

  /** All clusters currently known to the resolver. */
  def clusters: Seq[ClusterRef]

  /**
   * Picks the cluster for a session. Returning None means the user is not
   * allowed anywhere and the session must be rejected - never silently
   * redirected to some default, which would turn a routing mistake into an
   * access control bypass.
   */
  def resolve(user: String, sessionConf: Map[String, String]): Option[ClusterRef]
}
