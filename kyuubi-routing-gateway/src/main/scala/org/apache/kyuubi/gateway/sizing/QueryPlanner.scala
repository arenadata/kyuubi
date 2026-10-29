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

package org.apache.kyuubi.gateway.sizing

/**
 * Asks a cluster what a statement would cost before running it.
 *
 * This is per session rather than per gateway because the planner is the
 * cluster's own: the estimate depends on the catalog, the schema and the
 * statistics that session sees, and asking a different cluster would produce a
 * number for a query nobody is going to run.
 */
trait QueryPlanner {

  /**
   * Returns the planner's estimate of the statement as a JSON document.
   *
   * Failure is expressed by throwing: the caller decides what an unplannable
   * statement means, and for admission that is "size it as unknown", not
   * "refuse it".
   */
  def explain(statement: String): String
}

object QueryPlanner {

  /** Used where sizing is deliberately not wanted - routing without admission. */
  val none: QueryPlanner = (_: String) => ""
}
