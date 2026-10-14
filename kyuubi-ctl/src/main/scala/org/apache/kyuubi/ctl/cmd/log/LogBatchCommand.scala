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
package org.apache.kyuubi.ctl.cmd.log

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.client.BatchRestApi
import org.apache.kyuubi.client.api.v1.dto.{Batch, OperationLog}
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.client.util.BatchUtils
import org.apache.kyuubi.ctl.CtlConf._
import org.apache.kyuubi.ctl.RestClientFactory.{withKyuubiInstanceRestClient, withKyuubiRestClient}
import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.opt.CliConfig
import org.apache.kyuubi.ctl.util.Render

class LogBatchCommand(
    cliConfig: CliConfig,
    batch: Option[Batch] = None,
    restConfigMap: JMap[String, Object] = null)
  extends Command[Batch](cliConfig) {

  def validate(): Unit = {
    if (normalizedCliConfig.batchOpts.batchId == null) {
      fail("Must specify batchId for log batch command.")
    }
  }

  def doRun(): Batch = {
    withKyuubiRestClient(normalizedCliConfig, restConfigMap, conf) { kyuubiRestClient =>
      val batchRestApi: BatchRestApi = new BatchRestApi(kyuubiRestClient)
      val batchId = normalizedCliConfig.batchOpts.batchId
      var from = math.max(normalizedCliConfig.batchOpts.from, 0)
      val size = normalizedCliConfig.batchOpts.size

      var done = false
      var batch = this.batch.getOrElse(batchRestApi.getBatchById(batchId))
      var appState = this.batch.map(_.getAppState).orNull
      val kyuubiInstance = batch.getKyuubiInstance

      withKyuubiInstanceRestClient(kyuubiRestClient, kyuubiInstance) { kyuubiInstanceRestClient =>
        var kyuubiInstanceBatchRestApi: BatchRestApi = new BatchRestApi(kyuubiInstanceRestClient)

        def retrieveOperationLog(): OperationLog = {
          val log =
            try {
              kyuubiInstanceBatchRestApi.getBatchLocalLog(batchId, from, size)
            } catch {
              // the scheme of the kyuubi instance is taken from the rest base url and may not
              // match it, e.g. an https base url of a TLS-terminating proxy in front of plain
              // http instances, while the rest base url redirects the request to the instance
              case e: KyuubiRestException if kyuubiInstanceBatchRestApi ne batchRestApi =>
                warn(s"Failed to fetch batch logs from kyuubi instance $kyuubiInstance, " +
                  s"fetching them via the rest base url: ${e.getMessage}")
                kyuubiInstanceBatchRestApi = batchRestApi
                kyuubiInstanceBatchRestApi.getBatchLocalLog(batchId, from, size)
            }
          from += log.getLogRowSet.size
          log.getLogRowSet.asScala.foreach(x => info(x))
          log
        }

        while (!done) {
          var log: OperationLog = null
          try {
            log = retrieveOperationLog()
            val (latestBatch, shouldFinishLog) =
              checkStatus(kyuubiInstanceBatchRestApi, batchId, log)
            batch = latestBatch
            done = shouldFinishLog
          } catch {
            case e: Exception =>
              val (latestBatch, shouldFinishLog) = checkStatus(batchRestApi, batchId, log)
              batch = latestBatch
              done = shouldFinishLog
              if (done) {
                error(s"Error fetching batch logs: ${e.getMessage}")
              }
          }

          if (!done) {
            if (!Option(log).exists(_.getRowCount > 0)) {
              Option(batch).foreach { batch =>
                info(s"Application report for ${batch.getAppId} (state: ${batch.getAppState})," +
                  s" batch id: $batchId (state: ${batch.getState})")
                if (appState != batch.getAppState && StringUtils.isNotBlank(batch.getAppId)) {
                  appState = batch.getAppState
                  val appDetails = Render.buildBatchAppInfo(batch).mkString("\t ", "\n\t ", "")
                  info(appDetails)
                }
              }
            }
            Thread.sleep(conf.get(CTL_BATCH_LOG_QUERY_INTERVAL))
          }
        }

        // if the batch failed, show remaining batch logs with timeout
        if (batch != null && BatchUtils.isTerminalState(
            batch.getState) && !BatchUtils.isFinishedState(batch.getState)) {
          val startTime = System.currentTimeMillis()
          val timeout = conf.get(CTL_BATCH_LOG_ON_FAILURE_TIMEOUT)
          var hasRemainingLogs = true
          while (hasRemainingLogs && System.currentTimeMillis() - startTime < timeout) {
            try {
              if (retrieveOperationLog().getLogRowSet.size == 0) {
                hasRemainingLogs = false
              }
            } catch {
              case e: Exception =>
                error(s"Error fetching batch logs: ${e.getMessage}")
                hasRemainingLogs = false
            }
          }
          if (hasRemainingLogs) {
            info(s"See the complete logs with command `kyuubi-ctl log batch $batchId --forward`.")
          }
        }
      }
      batch
    }
  }

  def render(batch: Batch): Unit = {
    if (normalizedCliConfig.logOpts.forward) {
      info(Render.renderBatchInfo(batch))
    }
  }

  private def checkStatus(
      batchRestApi: BatchRestApi,
      batchId: String,
      log: OperationLog): (Batch, Boolean) = {
    var batch: Batch = null

    if (!normalizedCliConfig.logOpts.forward) {
      return (batch, true)
    }

    if (normalizedCliConfig.batchOpts.waitCompletion) {
      if (log == null || log.getLogRowSet.size == 0) {
        batch = batchRestApi.getBatchById(batchId)
        if (BatchUtils.isTerminalState(batch.getState)) {
          return (batch, true)
        }
      }
    } else {
      batch = batchRestApi.getBatchById(batchId)
      if (!BatchUtils.isPendingState(batch.getState)) {
        return (batch, true)
      }
    }

    (batch, false)
  }
}
