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

package org.apache.kyuubi.gateway.scaling

import scala.util.control.NonFatal

import okhttp3.{MediaType, OkHttpClient, Request, RequestBody}

import org.apache.kyuubi.Logging

/**
 * A worker's own view of whether it should still be given work.
 *
 * Trino's states are what make shrinking safe at all. `DRAINING` is announced
 * to the coordinator before anything is drained - the worker waits a grace
 * period so the coordinator observes it and stops assigning tasks - and it is
 * reversible, so a decision to shrink can be taken back when load returns.
 * `DRAINED` means every task on that worker has finished and it can be stopped
 * without failing a query.
 */
trait WorkerDrain {

  /** Asks the worker to stop taking new tasks and finish what it has. */
  def drain(workerUrl: String): Unit

  /** Puts a draining worker back to work. */
  def undrain(workerUrl: String): Unit

  /** The worker's current state, or None when it could not be asked. */
  def state(workerUrl: String): Option[String]
}

/** Talks to the worker over its own REST endpoint. */
class TrinoWorkerDrain(client: OkHttpClient) extends WorkerDrain with Logging {

  import TrinoWorkerDrain._

  override def drain(workerUrl: String): Unit = transition(workerUrl, Draining)

  override def undrain(workerUrl: String): Unit = transition(workerUrl, Active)

  override def state(workerUrl: String): Option[String] = {
    val request = new Request.Builder().url(stateUrl(workerUrl)).get().build()
    try {
      val response = client.newCall(request).execute()
      try {
        if (response.isSuccessful) {
          // The body is a quoted JSON string, e.g. "DRAINED".
          Option(response.body).map(_.string().trim.stripPrefix("\"").stripSuffix("\""))
        } else {
          warn(s"Could not read the state of $workerUrl: HTTP ${response.code}")
          None
        }
      } finally response.close()
    } catch {
      case NonFatal(e) =>
        warn(s"Could not read the state of $workerUrl: ${e.getMessage}")
        None
    }
  }

  private def transition(workerUrl: String, state: String): Unit = {
    val body = RequestBody.create(Json, "\"" + state + "\"")
    val request = new Request.Builder().url(stateUrl(workerUrl)).put(body).build()
    val response = client.newCall(request).execute()
    try {
      if (!response.isSuccessful) {
        throw new IllegalStateException(
          s"$workerUrl refused the transition to $state: HTTP ${response.code}")
      }
      info(s"$workerUrl is now $state")
    } finally response.close()
  }

  private def stateUrl(workerUrl: String): String = s"${workerUrl.stripSuffix("/")}$StatePath"
}

object TrinoWorkerDrain {

  val StatePath = "/v1/info/state"

  val Draining = "DRAINING"
  val Drained = "DRAINED"
  val Active = "ACTIVE"

  private val Json = MediaType.parse("application/json")
}
