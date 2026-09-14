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

package org.apache.kyuubi.gateway.session

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.jdbc.operation.JdbcOperationManager
import org.apache.kyuubi.engine.jdbc.session.JdbcSessionImpl
import org.apache.kyuubi.gateway.cluster.{ClusterRef, ClusterResolver}
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

/**
 * Routes to any JDBC-reachable engine. Impala is the case this was added for -
 * `ImpalaDialect` ships with the engine, so no dialect work is needed here.
 *
 * The dialect is picked from `kyuubi.engine.jdbc.type` when the cluster sets it,
 * otherwise from the url scheme: `jdbc:impala://...` resolves to impala.
 */
class JdbcRoutingSessionManager(resolver: ClusterResolver, override val engine: String)
  extends RoutingSessionManager("JdbcRoutingSessionManager", resolver) {

  // lazy: JdbcOperationManager needs conf, which is only set during initialize
  override lazy val operationManager: OperationManager = new JdbcOperationManager(conf)

  override protected def connectionConf(cluster: ClusterRef, user: String): Map[String, String] =
    Map(
      KyuubiConf.ENGINE_JDBC_CONNECTION_URL.key ->
        JdbcRoutingSessionManager.impersonate(cluster.url, user, templateFor(cluster)),
      KyuubiConf.ENGINE_JDBC_SHORT_NAME.key -> engine)

  /**
   * How this cluster expects to be told who is asking.
   *
   * Per cluster first, because a gateway can front drivers that spell it
   * differently - the Hive driver takes `hive.server2.proxy.user`, Cloudera's
   * Impala driver takes `DelegationUID` - and the gateway should not have to be
   * split in two for that.
   */
  private def templateFor(cluster: ClusterRef): Option[String] =
    cluster.sessionConf.get(JdbcRoutingSessionManager.IMPERSONATION_TEMPLATE_KEY)
      .orElse(conf.getOption(JdbcRoutingSessionManager.IMPERSONATION_TEMPLATE_KEY))
      .orElse(Some(JdbcRoutingSessionManager.DEFAULT_IMPERSONATION_TEMPLATE))
      .map(_.trim)
      .filter(_.nonEmpty)

  override protected def createEngineSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      cluster: ClusterRef): Session =
    // Admission is not wired for the JDBC path yet: Impala does its own
    // admission control, so double-accounting it needs thought rather than a
    // copy of the Trino branch.
    new JdbcSessionImpl(protocol, user, password, ipAddress, conf, this)
}

object JdbcRoutingSessionManager {

  /**
   * How the caller's identity is appended to the connection url.
   *
   * `{user}` is replaced with the authenticated user. Set it empty to connect
   * as the gateway's own account - which means the cluster sees one identity
   * for everybody, and its per-user authorisation stops meaning anything.
   */
  val IMPERSONATION_TEMPLATE_KEY = "kyuubi.gateway.jdbc.impersonationTemplate"

  /**
   * On by default, unlike admission and scaling.
   *
   * Those change what a working deployment does; this one decides whether the
   * cluster is told who is asking at all. Defaulting it off would make every
   * query arrive as the gateway's service account and collapse Impala's
   * per-user authorisation silently, whereas defaulting it on fails loudly and
   * immediately if the gateway is not an authorised proxy user - which is a
   * configuration the operator can then fix.
   */
  val DEFAULT_IMPERSONATION_TEMPLATE = ";hive.server2.proxy.user={user}"

  private val UserPlaceholder = "{user}"

  /**
   * Appends the identity to the url, leaving one that is already there alone.
   *
   * A cluster url that already names a proxy user was written that way on
   * purpose; appending a second one would produce a string whose meaning
   * depends on which the driver reads first.
   */
  def impersonate(url: String, user: String, template: Option[String]): String = template match {
    case None => url
    case Some(t) =>
      // The user reaches here as an authenticated identity, but the template
      // splices it into a connection string with no quoting available - a
      // `;` or `&` in it would add driver parameters nobody configured,
      // potentially including a second, attacker-chosen proxy-user parameter
      // that overrides the very impersonation this template exists to
      // enforce. Rejected rather than escaped: there is no quoting convention
      // shared by every JDBC driver this can front.
      if (!SafeUser.pattern.matcher(user).matches()) {
        throw KyuubiSQLException(
          s"User '$user' cannot be used to impersonate on a JDBC connection: " +
            s"only ${SafeUser.pattern} is allowed")
      }
      val parameter = t.takeWhile(c => c != '=').stripPrefix(";").stripPrefix("?").stripPrefix("&")
      if (parameter.nonEmpty && url.contains(parameter)) url
      else url + t.replace(UserPlaceholder, user)
  }

  /** Conservative on purpose: every identity provider this fronts fits it. */
  private val SafeUser = "[A-Za-z0-9._@-]+".r
}
