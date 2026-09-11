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

package org.apache.kyuubi.gateway

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols.FrontendProtocol
import org.apache.kyuubi.server.{KyuubiTBinaryFrontendService, KyuubiTHttpFrontendService}
import org.apache.kyuubi.service.{AbstractBackendService, AbstractFrontendService, Serverable}

/**
 * Composition root of the gateway.
 *
 * The whole architecture is this class: a backend service that routes by
 * identity, and the stock frontends layered over it. No engine is launched,
 * so query results traverse one intermediary rather than two.
 */
class RoutingGateway(name: String) extends Serverable(name) {

  def this() = this(classOf[RoutingGateway].getSimpleName)

  override val backendService: AbstractBackendService = new RoutingBackendService()

  override lazy val frontendServices: Seq[AbstractFrontendService] = {
    val protocols = conf.get(KyuubiConf.FRONTEND_PROTOCOLS).map(FrontendProtocols.withName)
    val services = ListBuffer[AbstractFrontendService]()
    protocols.foreach {
      case FrontendProtocols.THRIFT_BINARY =>
        services += new KyuubiTBinaryFrontendService(this)
      case FrontendProtocols.THRIFT_HTTP =>
        services += new KyuubiTHttpFrontendService(this)
      case other: FrontendProtocol =>
        warn(s"Frontend protocol $other is not wired in the gateway yet, ignoring")
    }
    if (services.isEmpty) {
      throw new IllegalArgumentException(
        s"No supported frontend protocol in ${KyuubiConf.FRONTEND_PROTOCOLS.key}")
    }
    services.toSeq
  }

  override protected def stopServer(): Unit = {
    info(s"$name stopped")
  }
}
