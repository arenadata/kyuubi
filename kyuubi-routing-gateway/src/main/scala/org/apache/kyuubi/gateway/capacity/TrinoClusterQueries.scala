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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import okhttp3.{OkHttpClient, Request}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.gateway.cluster.ClusterRef
import org.apache.kyuubi.gateway.session.GatedTrinoSession

/**
 * Reads the coordinator's list of queries over its own REST endpoint.
 *
 * `/v1/query` rather than an event listener plugin: the plugin would have to be
 * installed and configured on every cluster the gateway fronts, and a gateway
 * that only works against clusters someone remembered to configure is a gateway
 * that silently over-admits the rest. Polling needs nothing on the far side.
 *
 * The cost is that a query is only visible while the coordinator still holds
 * it. That is exactly the signal wanted here - a query that has aged out of the
 * coordinator's history is certainly finished - so the weakness of the endpoint
 * is what makes it usable.
 */
class TrinoClusterQueries(client: OkHttpClient, user: String)
  extends ClusterQueries with Logging {

  import TrinoClusterQueries._

  override def observe(cluster: ClusterRef): Option[Seq[ObservedQuery]] = {
    val request = new Request.Builder()
      .url(s"${cluster.url.stripSuffix("/")}$QueriesPath")
      .header("X-Trino-User", user)
      .get()
      .build()
    try {
      val response = client.newCall(request).execute()
      try {
        if (!response.isSuccessful) {
          warn(s"Could not list queries on ${cluster.name}: HTTP ${response.code}")
          None
        } else {
          Some(parse(Option(response.body).map(_.string()).getOrElse("[]")))
        }
      } finally response.close()
    } catch {
      case NonFatal(e) =>
        warn(s"Could not list queries on ${cluster.name}: ${e.getMessage}")
        None
    }
  }
}

object TrinoClusterQueries extends Logging {

  val QueriesPath = "/v1/query"

  /**
   * States in which the coordinator is done with a query.
   *
   * Matched by name rather than by an enum from the server: the gateway has no
   * dependency on trino-main, and an unknown state is treated as still running,
   * which errs towards holding a reservation slightly too long rather than
   * releasing capacity that is still in use.
   */
  private val Terminal = Set("FINISHED", "FAILED", "CANCELED")

  private val mapper = new ObjectMapper()

  def parse(json: String): Seq[ObservedQuery] = {
    val root = mapper.readTree(json)
    if (!root.isArray) return Seq.empty
    root.elements().asScala.toSeq.flatMap(observed)
  }

  private def observed(query: JsonNode): Option[ObservedQuery] = {
    val tags = Option(query.path("session").get("clientTags"))
      .filter(_.isArray)
      .map(_.elements().asScala.toSeq.map(_.asText()))
      .getOrElse(Seq.empty)

    GatedTrinoSession.reservationOf(tags).map { reservationId =>
      val state = Option(query.path("state").asText(null)).getOrElse("")
      ObservedQuery(
        reservationId = reservationId,
        finished = Terminal.contains(state),
        peakMemoryBytes = peakMemory(query))
    }
  }

  /**
   * The peak the query actually reached, when the coordinator reports one.
   *
   * Trino serialises data sizes either as a number of bytes or as a string like
   * "1.50GB" depending on the field and the release, so both are read. A size
   * that cannot be understood is dropped rather than guessed: it feeds
   * calibration, and a wrong number there would quietly bias every future
   * estimate.
   */
  private def peakMemory(query: JsonNode): Option[Long] = {
    val stats = query.path("queryStats")
    Seq("peakUserMemoryReservation", "peakMemoryBytes", "peakTotalMemoryReservation")
      .map(stats.get)
      .find(_ != null)
      .flatMap {
        case n if n.isNumber => Some(n.asLong())
        case n if n.isTextual => parseDataSize(n.asText())
        case _ => None
      }
      .filter(_ >= 0)
  }

  private val DataSize = """^\s*([0-9.]+)\s*([kKmMgGtTpP]?)[bB]\s*$""".r

  private val unitScale = Map(
    "" -> 1L,
    "k" -> 1024L,
    "m" -> (1024L * 1024),
    "g" -> (1024L * 1024 * 1024),
    "t" -> (1024L * 1024 * 1024 * 1024),
    "p" -> (1024L * 1024 * 1024 * 1024 * 1024))

  def parseDataSize(text: String): Option[Long] = text match {
    case DataSize(value, unit) =>
      for {
        amount <- scala.util.Try(value.toDouble).toOption
        scale <- unitScale.get(unit.toLowerCase)
      } yield (amount * scale).toLong
    case _ => None
  }
}
