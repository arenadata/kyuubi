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

import org.apache.kyuubi.gateway.cluster.ClusterRef

/**
 * What a query the gateway admitted turned out to cost.
 *
 * @param reservationId the id the gateway admitted it under
 * @param finished      whether the cluster considers it over
 * @param peakMemoryBytes what it actually held at its peak, when the cluster says
 */
case class ObservedQuery(
    reservationId: String,
    finished: Boolean,
    peakMemoryBytes: Option[Long])

/**
 * The cluster's own account of what it is running.
 *
 * The gateway's record of what it admitted is a belief; this is the fact. They
 * diverge whenever a release is missed - a client that vanishes mid-query, a
 * gateway restarted while queries were in flight - and a reservation that
 * outlives its query shrinks the cluster's apparent capacity for good.
 */
trait ClusterQueries {

  /**
   * Queries the cluster knows about that the gateway admitted.
   *
   * None means the cluster could not be asked. That is deliberately different
   * from an empty answer: "nothing is running" would justify releasing every
   * reservation, and an unreachable coordinator must never do that.
   */
  def observe(cluster: ClusterRef): Option[Seq[ObservedQuery]]
}
