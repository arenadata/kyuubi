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

package org.apache.kyuubi.plugin.lineage.detailed

import scala.util.control.NonFatal

import org.apache.kyuubi.Logging
import org.apache.kyuubi.events.KyuubiEvent
import org.apache.kyuubi.events.handler.EventHandler
import org.apache.kyuubi.plugin.lineage.dispatcher.OperationLineageKyuubiDetailedEvent

class KyuubiDetailedEventHandler(val lineageLogger: LineageLogger)
  extends EventHandler[KyuubiEvent]
  with Logging {

  override def apply(event: KyuubiEvent): Unit = {
    event match {
      case lineageEvent: OperationLineageKyuubiDetailedEvent =>
        handleLineageEvent(lineageEvent)
      case _ =>
    }
  }

  private def handleLineageEvent(lineageEvent: OperationLineageKyuubiDetailedEvent): Unit = {
    try {
      lineageEvent.lineage
        .filter { l =>
          l.inputTables.nonEmpty || l.outputTables.nonEmpty
        }
        .foreach(lineageLogger.log(lineageEvent.execution, _))
    } catch {
      case NonFatal(exception: Exception) => error(
          s"Error during handling lineage event for ${lineageEvent.execution.id}",
          exception)
    }
  }
}
