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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.trino.{TrinoContext, TrinoStatement}

/**
 * Plans a statement on the Trino cluster the session is already connected to.
 *
 * The statement is not parsed or rewritten here, only prefixed. Trino's own
 * parser decides what is plannable, which is the only authority that agrees
 * with what will happen at execution time.
 *
 * `context` is by name because the session assigns it in `open()`, after the
 * planner is constructed; nothing plans before then.
 */
class TrinoQueryPlanner(context: => TrinoContext, conf: KyuubiConf)
  extends QueryPlanner with Logging {

  import TrinoQueryPlanner._

  override def explain(statement: String): String = {
    val trinoStatement = TrinoStatement(context, conf, explainSql(statement))
    try firstCell(trinoStatement.execute())
    finally {
      // The statement holds a status printer thread whether or not it printed.
      trinoStatement.stopPrinter()
    }
  }
}

object TrinoQueryPlanner {

  /**
   * `TYPE DISTRIBUTED` is the point: the logical plan carries no estimates
   * worth sizing against, and only the distributed plan shows what each
   * fragment is expected to hold. `FORMAT JSON` because the text rendering is
   * meant for people and changes between releases.
   */
  def explainSql(statement: String): String =
    s"EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) ${statement.trim.stripSuffix(";")}"

  /**
   * The plan is a single row of a single column.
   *
   * Taking the first cell rather than concatenating: more than one row would
   * mean Trino changed the shape of EXPLAIN output, and gluing the pieces
   * together would produce a document the parser would misread as a plan.
   */
  def firstCell(rows: Iterator[List[Any]]): String =
    if (!rows.hasNext) ""
    else
      rows.next() match {
        case (cell: String) :: _ => cell
        case cell :: _ if cell != null => cell.toString
        case _ => ""
      }
}
