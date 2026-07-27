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

package org.apache.kyuubi.server

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service}
import org.apache.kyuubi.server.flight.{
  KyuubiFlightAuthHandler,
  KyuubiFlightSqlProducer}
import org.apache.kyuubi.util.JavaUtils

class KyuubiFlightSqlFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiFlightSqlFrontendService") with Logging {

  private var allocator: BufferAllocator = _
  private var producer: KyuubiFlightSqlProducer = _
  private var flightServer: FlightServer = _
  private var configuredPort: Int = _

  private val started = new AtomicBoolean(false)

  private lazy val host: String = conf.get(FRONTEND_FLIGHT_SQL_BIND_HOST).getOrElse {
    if (conf.get(FRONTEND_CONNECTION_URL_USE_HOSTNAME)) {
      JavaUtils.findLocalInetAddress.getCanonicalHostName
    } else {
      JavaUtils.findLocalInetAddress.getHostAddress
    }
  }

  private def configuredLocation: Location =
    Location.forGrpcInsecure(host, configuredPort)

  private def currentLocation: Location = {
    val advertisedHost = conf.get(FRONTEND_ADVERTISED_HOST).getOrElse(host)
    if (flightServer != null && started.get()) {
      Location.forGrpcInsecure(advertisedHost, flightServer.getPort)
    } else {
      Location.forGrpcInsecure(advertisedHost, configuredPort)
    }
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    configuredPort = conf.get(FRONTEND_FLIGHT_SQL_BIND_PORT)
    allocator = new RootAllocator()
    producer = new KyuubiFlightSqlProducer(
      serverable.backendService,
      allocator,
      () => currentLocation,
      conf)
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    if (!started.get()) {
      try {
        if (conf.get(FRONTEND_FLIGHT_SQL_SSL_ENABLED)) {
          throw new IllegalArgumentException(
            "Arrow Flight SQL TLS requires PEM certificate and key files; " +
              "the current Kyuubi keystore settings are not supported by Arrow Flight 16")
        }
        flightServer = FlightServer
          .builder(allocator, configuredLocation, producer)
          .headerAuthenticator(new KyuubiFlightAuthHandler(conf))
          .backpressureThreshold(10 * 1024 * 1024)
          .build()
          .start()
        started.set(true)
        info(s"Flight SQL frontend service started at $connectionUrl")
      } catch {
        case NonFatal(e) =>
          if (flightServer != null) {
            try flightServer.close() catch {
              case NonFatal(closeError) =>
                warn("Failed to close Flight SQL server after startup failure", closeError)
            }
            flightServer = null
          }
          throw new KyuubiException("Cannot start Flight SQL frontend service", e)
      }
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    if (started.getAndSet(false)) {
      if (producer != null) {
        try producer.close() catch {
          case NonFatal(e) => warn("Failed to close Flight SQL producer", e)
        }
      }
      if (flightServer != null) {
        try flightServer.close() catch {
          case NonFatal(e) => warn("Failed to close Flight SQL server", e)
        }
        flightServer = null
      }
      if (allocator != null) {
        try allocator.close() catch {
          case NonFatal(e) => warn("Failed to close Flight SQL allocator", e)
        }
        allocator = null
      }
    }
    super.stop()
  }

  override def connectionUrl: String = {
    checkInitialized()
    val advertisedHost = conf.get(FRONTEND_ADVERTISED_HOST).getOrElse(host)
    s"$advertisedHost:${if (flightServer != null) flightServer.getPort else configuredPort}"
  }

  override val discoveryService: Option[Service] = None
}
