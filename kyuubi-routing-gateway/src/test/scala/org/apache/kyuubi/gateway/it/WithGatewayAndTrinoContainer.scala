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

package org.apache.kyuubi.gateway.it

import java.net.ServerSocket

import com.dimafeng.testcontainers.TrinoContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.testcontainers.utility.DockerImageName

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.gateway.RoutingGateway

/**
 * A gateway in front of a real Trino.
 *
 * Everything up to here is either a unit test or a fake: the planner was proved
 * against a coordinator that speaks the client protocol, and routing against a
 * gateway with no cluster behind it. What neither can show is whether a query
 * sent by a JDBC client comes back with the right answer, which is the only
 * thing the gateway exists to do.
 *
 * The cluster is declared statically rather than discovered. Discovery is
 * covered elsewhere and would drag a Kubernetes api server into a test about
 * queries; what is under test here is the path from client to cluster.
 */
trait WithGatewayAndTrinoContainer extends KyuubiFunSuite with TestContainerForAll {

  final val IMAGE_VERSION = 411
  final val DOCKER_IMAGE_NAME = s"trinodb/trino:$IMAGE_VERSION"

  override val containerDef: TrinoContainer.Def =
    TrinoContainer.Def(DockerImageName.parse(DOCKER_IMAGE_NAME))

  protected val catalog = "memory"
  protected val schema = "default"

  /** Overridden by a suite that needs admission, scaling or another frontend. */
  protected def gatewayConf(trinoUrl: String): KyuubiConf = baseConf(trinoUrl)

  protected def baseConf(trinoUrl: String): KyuubiConf = KyuubiConf()
    .set(KyuubiConf.FRONTEND_PROTOCOLS.key, "THRIFT_BINARY,THRIFT_HTTP")
    .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_HOST.key, "localhost")
    .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT.key, "0")
    .set(KyuubiConf.FRONTEND_THRIFT_HTTP_BIND_HOST.key, "localhost")
    // A real port, not 0. The HTTP frontend reports the port it was configured
    // with rather than the one it bound, so a 0 there reaches the client as a
    // literal 0 - which the driver reads as "unspecified" and replaces with the
    // default 10009, connecting to the binary frontend over HTTP and failing in
    // a way that looks like the gateway is down.
    .set(KyuubiConf.FRONTEND_THRIFT_HTTP_BIND_PORT.key, httpPort.toString)
    .set("kyuubi.gateway.cluster.analytics.url", trinoUrl)
    .set("kyuubi.gateway.cluster.analytics.default", "true")
    .set(
      "kyuubi.gateway.cluster.analytics.session." +
        KyuubiConf.ENGINE_TRINO_CONNECTION_CATALOG.key,
      catalog)

  /**
   * A port that was free a moment ago.
   *
   * Racy in principle and fine here: the window is microseconds and the
   * alternative is a fixed port that collides with whatever else the machine
   * is running.
   */
  private lazy val httpPort: Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }

  @volatile private var gateway: RoutingGateway = _

  /** `http://host:port` of the container, as the Trino client wants it. */
  @volatile protected var trinoUrl: String = _

  protected def binaryUrl: String = frontendUrl("KyuubiTBinaryFrontend")

  /**
   * The HTTP transport's url, with the parameters the driver needs.
   *
   * The path has to match `kyuubi.frontend.thrift.http.path`, whose default is
   * `cliservice`; a mismatch shows as a connection that opens and then answers
   * 404 to everything, which is not obviously a path problem.
   */
  protected def httpUrl: String =
    s"${frontendUrl("KyuubiTHttpFrontendService")}/;transportMode=http;httpPath=cliservice"

  private def frontendUrl(name: String): String = {
    val service = gateway.frontendServices.find(_.getName == name).getOrElse {
      throw new IllegalStateException(s"$name is not running")
    }
    s"jdbc:hive2://${service.connectionUrl}"
  }

  override def beforeAll(): Unit = {
    withContainers { container =>
      // The container reports a JDBC url; the Trino client wants the bare
      // server address.
      trinoUrl = container.jdbcUrl.replace("jdbc:trino", "http").split("/").take(3).mkString("/")
      gateway = new RoutingGateway()
      gateway.initialize(gatewayConf(trinoUrl))
      gateway.start()
      super.beforeAll()
    }
  }

  override def afterAll(): Unit = {
    if (gateway != null) gateway.stop()
    super.afterAll()
  }
}
