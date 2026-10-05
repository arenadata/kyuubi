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

import org.apache.kyuubi.gateway.cluster.ScaleTarget

/**
 * Raises a cluster's worker count.
 *
 * Only upwards. Lowering it here would delete pods, and a Trino worker that
 * disappears takes the query fragments running on it with it - shrinking safely
 * means draining first, which is the operator's business and not expressible
 * through a replica count. The gateway asks for more and lets whatever owns the
 * cluster's lifecycle decide when there is too much.
 */
trait ClusterScaler {

  /**
   * Ensures the target is sized for at least `workers`, and reports the size it
   * will have.
   *
   * Returning the size rather than success: the caller is about to admit a
   * query against it, and needs the number it can count on - which is the
   * current size when that is already enough, and may be less than asked for
   * if the request could not be made.
   */
  def ensureAtLeast(target: ScaleTarget, workers: Int): Int
}
