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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{HDFS_LOGGER_ROOT, HDFS_LOGGER_URL}
import org.apache.kyuubi.events.KyuubiEvent
import org.apache.kyuubi.events.handler.{CustomEventHandlerProvider, EventHandler}
import org.apache.kyuubi.util.KyuubiHadoopUtils

class KyuubiDetailedEventHandlerProvider extends CustomEventHandlerProvider {

  override def create(kyuubiConf: KyuubiConf): EventHandler[KyuubiEvent] = {
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    val hdfsUrl = kyuubiConf.getOption(HDFS_LOGGER_URL.key).getOrElse {
      throw new IllegalArgumentException(s"Option ${HDFS_LOGGER_URL.key} should be set")
    }
    hadoopConf.set("fs.defaultFS", hdfsUrl)

    new KyuubiDetailedEventHandler(
      new HdfsLineageLogger(kyuubiConf.get(HDFS_LOGGER_ROOT), hadoopConf))
  }
}
