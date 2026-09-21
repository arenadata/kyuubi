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

package org.apache.kyuubi.server.api.v1

import java.sql.{Connection, DriverManager}
import javax.ws.rs.client.Entity
import javax.ws.rs.core.{GenericType, MediaType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.apache.kyuubi.{RestFrontendTestHelper, Utils}
import org.apache.kyuubi.client.api.v1.dto.{EngineProfileGroup, SessionOpenCount, SessionOpenRequest}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols.FrontendProtocol
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_ID, KYUUBI_ENGINE_PROFILE_NAME_KEY}
import org.apache.kyuubi.metrics.MetricsConf.METRICS_PROMETHEUS_PORT
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.server.{KyuubiRestFrontendService, KyuubiTBinaryFrontendService}
import org.apache.kyuubi.server.http.util.HttpAuthUtils
import org.apache.kyuubi.server.http.util.HttpAuthUtils.AUTHORIZATION_HEADER
import org.apache.kyuubi.service.authentication.AnonymousAuthenticationProviderImpl

class EngineProfileRestSuite extends RestFrontendTestHelper with HiveJDBCTestHelper {

  override protected val frontendProtocols: Seq[FrontendProtocol] =
    Seq(FrontendProtocols.THRIFT_BINARY, FrontendProtocols.REST)

  override protected lazy val conf: KyuubiConf = {
    val result = KyuubiConf(false)
      .set(METRICS_PROMETHEUS_PORT, 0)
      .set(AUTHENTICATION_METHOD, Seq("CUSTOM"))
      .set(AUTHENTICATION_CUSTOM_CLASS, classOf[AnonymousAuthenticationProviderImpl].getName)
      .set(SERVER_ADMINISTRATORS, Set(Utils.currentUser))
      .set(ENGINE_SHARE_LEVEL, "USER")
      .set(ENGINE_DO_AS_ENABLED, false)
      .set(SESSION_ENGINE_LAUNCH_ASYNC, false)
      .set(SESSION_CONF_RESTRICT_LIST, Set("spark.sql.adaptive.enabled"))
      .set("spark.master", "local[2]")
      .set("spark.driver.memory", "512m")
      .set(s"___${Utils.currentUser}___.kyuubi.engine.profiles.blacklist", "idle")
    Seq("first", "second", "idle").foreach { name =>
      result.set(s"kyuubi.engine.profile.$name.type", "SPARK_SQL")
    }
    result
  }

  override protected lazy val fe: KyuubiRestFrontendService =
    server.frontendServices.collectFirst {
      case frontend: KyuubiRestFrontendService => frontend
    }.getOrElse(fail("REST frontend was not started"))

  override protected def jdbcUrl: String = {
    val frontend = server.frontendServices.collectFirst {
      case thrift: KyuubiTBinaryFrontendService => thrift
    }.getOrElse(fail("Thrift binary frontend was not started"))
    s"jdbc:hive2://${frontend.connectionUrl}/;"
  }

  test("ADH-8475: REST groups real JDBC engines by their resolved profile") {
    val connections = ArrayBuffer.empty[Connection]
    try {
      val expected = Seq("first", "second", "<none>").map { profile =>
        val configs = if (profile == "<none>") Map.empty[String, String]
        else Map(ENGINE_PROFILE.key -> profile)
        val connection = withSessionConf()(configs)() {
          DriverManager.getConnection(jdbcUrlWithConf, user, password)
        }
        connections += connection
        profile -> engineId(connection)
      }.toMap
      assert(expected.values.toSet.size === 3)

      eventually(timeout(30.seconds), interval(200.millis)) {
        val groups = listProfiles().map(group => group.getProfile -> group).toMap
        expected.foreach { case (profile, id) =>
          val group = groups.getOrElse(profile, fail(s"Missing profile $profile: $groups"))
          assert(group.getEngineType === "SPARK_SQL")
          assert(group.getStatus === "RUNNING")
          assert(group.getInstanceCount === 1)
          assert(group.getEngines.size() === 1)
          val engine = group.getEngines.get(0)
          assert(engine.getUser === user)
          assert(engine.getSharelevel === "USER")
          assert(engine.getInstance.nonEmpty)
          assert(engine.getNamespace.nonEmpty)
          assert(engine.getAttributes.get(KYUUBI_ENGINE_ID) === id)
          if (profile == "<none>") {
            assert(!engine.getAttributes.containsKey(KYUUBI_ENGINE_PROFILE_NAME_KEY))
          } else {
            assert(engine.getAttributes.get(KYUUBI_ENGINE_PROFILE_NAME_KEY) === profile)
          }
        }
        val idle = groups("idle")
        assert(idle.getEngineType === "SPARK_SQL")
        assert(idle.getStatus === "IDLE")
        assert(idle.getInstanceCount === 0)
        assert(idle.getEngines.isEmpty)
      }
    } finally {
      connections.reverseIterator.foreach(_.close())
    }
  }

  test("ADH-8475/ADH-9238: rejected REST opens preserve the reason and session count") {
    val before = sessionCount()
    val requests = Seq(
      Map(ENGINE_PROFILE.key -> "missing") -> "Engine profile 'missing' is not defined",
      Map(ENGINE_PROFILE.key -> "idle") ->
        s"Current user '$user' is not allowed to use the engine profile 'idle'",
      Map(ENGINE_PROFILE.key -> "first", "spark.sql.adaptive.enabled" -> "true") ->
        "spark.sql.adaptive.enabled is a restrict key according to the server-side configuration")

    requests.foreach { case (configs, reason) =>
      val response = webTarget.path("api/v1/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(user))
        .post(Entity.entity(
          new SessionOpenRequest(configs.asJava),
          MediaType.APPLICATION_JSON_TYPE))
      try {
        assert(response.getStatus === 500)
        val error = response.readEntity(new GenericType[java.util.Map[String, String]]() {})
        assert(error.get("message").contains(reason), error.toString)
      } finally {
        response.close()
      }
      assert(sessionCount() === before)
    }
  }

  private def engineId(connection: Connection): String = {
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery("SELECT engine_id()")
      try {
        assert(result.next())
        val id = result.getString(1)
        assert(id.nonEmpty)
        assert(!result.next())
        id
      } finally {
        result.close()
      }
    } finally {
      statement.close()
    }
  }

  private def listProfiles(): Seq[EngineProfileGroup] = {
    val response = webTarget.path("api/v1/admin/engine/profile")
      .queryParam("sharelevel", "USER")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(user))
      .get()
    try {
      assert(response.getStatus === 200)
      response.readEntity(new GenericType[Seq[EngineProfileGroup]]() {})
    } finally {
      response.close()
    }
  }

  private def sessionCount(): Int = {
    val response = webTarget.path("api/v1/sessions/count")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, HttpAuthUtils.basicAuthorizationHeader(user))
      .get()
    try {
      assert(response.getStatus === 200)
      response.readEntity(classOf[SessionOpenCount]).getOpenSessionCount
    } finally {
      response.close()
    }
  }
}
