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

package org.apache.kyuubi.server.flight

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.arrow.flight.{CallHeaders, CallInfo, CallStatus, FlightServerMiddleware, RequestContext}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys._

/**
 * Captures `kyuubi.*` gRPC call headers as Kyuubi session configuration, the Flight SQL
 * counterpart of the `?key=value` part of a HiveServer2 JDBC URL.
 *
 * Clients set them as call headers, e.g. ADBC
 * `adbc.flight.sql.rpc.call_header.kyuubi.engine.profile=impala`, or the Arrow Flight SQL
 * JDBC driver, which forwards unknown URL parameters as headers:
 * `jdbc:arrow-flight-sql://host:10299/?kyuubi.engine.profile=impala`.
 *
 * The captured map is merged into the session configuration *before* the server-owned keys,
 * and the session manager still applies `kyuubi.session.conf.restrict.list` /
 * `kyuubi.session.conf.ignore.list` and the engine profile blacklist, exactly as for Thrift.
 */
class KyuubiFlightSessionConfMiddleware(val sessionConf: Map[String, String])
  extends FlightServerMiddleware {
  override def onBeforeSendingHeaders(outgoingHeaders: CallHeaders): Unit = {}
  override def onCallCompleted(status: CallStatus): Unit = {}
  override def onCallErrored(err: Throwable): Unit = {}
}

object KyuubiFlightSessionConfMiddleware {

  val KEY: FlightServerMiddleware.Key[KyuubiFlightSessionConfMiddleware] =
    FlightServerMiddleware.Key.of("kyuubi-session-conf")

  private val CONF_PREFIX = "kyuubi."

  /**
   * Keys a client must never choose: identity, routing and internal bookkeeping. Flight has
   * no impersonation check, so proxy-user keys are dropped as well.
   */
  private val RESERVED_KEYS: Set[String] = Set(
    KYUUBI_CLIENT_IP_KEY,
    KYUUBI_CLIENT_VERSION_KEY,
    KYUUBI_SERVER_IP_KEY,
    KYUUBI_SESSION_USER_KEY,
    KYUUBI_SESSION_SIGN_PUBLICKEY,
    KYUUBI_SESSION_USER_SIGN,
    KYUUBI_SESSION_REAL_USER_KEY,
    KYUUBI_SESSION_CONNECTION_URL_KEY,
    KYUUBI_BATCH_PRIORITY,
    KYUUBI_BATCH_RESOURCE_UPLOADED_KEY,
    KYUUBI_STATEMENT_ID_KEY,
    KYUUBI_ENGINE_ID,
    KYUUBI_ENGINE_NAME,
    KYUUBI_ENGINE_URL,
    KYUUBI_ENGINE_CONNECT_URL,
    KYUUBI_ENGINE_SUBMIT_TIME_KEY,
    KYUUBI_ENGINE_CREDENTIALS_KEY,
    KYUUBI_ENGINE_APP_MGR_INFO_KEY,
    KYUUBI_ENGINE_PROFILE_NAME_KEY,
    KYUUBI_SESSION_HANDLE_KEY,
    KYUUBI_SESSION_ALIVE_PROBE,
    KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_GUID,
    KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET,
    KYUUBI_SESSION_ENGINE_LAUNCH_SUPPORT_RESULT,
    KYUUBI_OPERATION_SET_CURRENT_CATALOG,
    KYUUBI_OPERATION_GET_CURRENT_CATALOG,
    KYUUBI_OPERATION_SET_CURRENT_DATABASE,
    KYUUBI_OPERATION_GET_CURRENT_DATABASE,
    KYUUBI_OPERATION_HANDLE_KEY,
    KyuubiConf.PROXY_USER.key,
    KyuubiConf.FRONTEND_PROTOCOLS.key).map(_.toLowerCase(Locale.ROOT))

  /**
   * Session configuration from call headers. gRPC header names are lower-cased on the wire,
   * so keys are mapped back to the registered Kyuubi config key where one exists.
   */
  def sessionConf(headers: Iterable[(String, String)]): Map[String, String] =
    headers.flatMap { case (name, value) =>
      val key = KyuubiConf.canonicalKey(name.trim)
      if (value != null &&
        key.toLowerCase(Locale.ROOT).startsWith(CONF_PREFIX) &&
        !RESERVED_KEYS.contains(key.toLowerCase(Locale.ROOT))) {
        Some(key -> value.trim)
      } else {
        None
      }
    }.toMap

  class Factory extends FlightServerMiddleware.Factory[KyuubiFlightSessionConfMiddleware] {
    override def onCallStarted(
        info: CallInfo,
        incomingHeaders: CallHeaders,
        context: RequestContext): KyuubiFlightSessionConfMiddleware = {
      val headers = incomingHeaders.keys().asScala
        .filterNot(_.endsWith("-bin")) // binary metadata cannot be a config value
        .map(name => name -> incomingHeaders.get(name))
      new KyuubiFlightSessionConfMiddleware(sessionConf(headers))
    }
  }
}
