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

package org.apache.kyuubi.service

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SSL_ENABLED
import org.apache.kyuubi.util.{EngineSslServerMaterial, EngineSslUtils}

/**
 * An engine-like Thrift binary frontend that supports engine TLS, see
 * [[TBinaryFrontendService.engineSslMaterial]]. It loads the key material from its own conf.
 */
class NoopSslTBinaryFrontendService(override val serverable: Serverable)
  extends TBinaryFrontendService("NoopSslThriftBinaryFrontend") {

  override val discoveryService: Option[Service] = None

  override protected def engineSslMaterial: Option[EngineSslServerMaterial] =
    if (conf.get(ENGINE_SSL_ENABLED)) Some(EngineSslUtils.loadServerMaterial(conf)) else None
}
