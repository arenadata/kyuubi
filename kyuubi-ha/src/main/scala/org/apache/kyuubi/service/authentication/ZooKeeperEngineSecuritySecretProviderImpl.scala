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

package org.apache.kyuubi.service.authentication

import java.nio.charset.StandardCharsets
import java.security.SecureRandom

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SECURITY_CRYPTO_KEY_LENGTH
import org.apache.kyuubi.ha.HighAvailabilityConf.{
  HA_ZK_ENGINE_SECURE_SECRET_NODE,
  HA_ZK_ENGINE_SECURE_SECRET_NODE_AUTO_CREATE,
  HA_ZK_NODE_TIMEOUT}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider

class ZooKeeperEngineSecuritySecretProviderImpl extends EngineSecuritySecretProvider {
  import DiscoveryClientProvider._
  import ZooKeeperEngineSecuritySecretProviderImpl._

  private var conf: KyuubiConf = _

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
  }

  override def getSecret(): String = {
    val zkNode = conf.get(HA_ZK_ENGINE_SECURE_SECRET_NODE)
    val autoCreate = conf.get(HA_ZK_ENGINE_SECURE_SECRET_NODE_AUTO_CREATE)
    withDiscoveryClient[String](conf) { discoveryClient =>
      // The existence check and the create+setData below must all happen under the same lock:
      // otherwise a concurrent reader could observe the node right after create() but before
      // setData(), i.e. existing but still empty, and use that as its secret.
      discoveryClient.tryWithLock(s"${zkNode}_lock", conf.get(HA_ZK_NODE_TIMEOUT)) {
        if (discoveryClient.pathExists(zkNode)) {
          new String(discoveryClient.getData(zkNode), StandardCharsets.UTF_8)
        } else if (!autoCreate) {
          throw new IllegalArgumentException(
            s"ZooKeeper node $zkNode does not exist and " +
              s"${HA_ZK_ENGINE_SECURE_SECRET_NODE_AUTO_CREATE.key} is false; please create it " +
              "manually before enabling engine security, or leave auto-create enabled.")
        } else {
          val secret = generateSecret(conf)
          discoveryClient.create(zkNode, "PERSISTENT")
          discoveryClient.setData(zkNode, secret.getBytes(StandardCharsets.UTF_8))
          secret
        }
      }
    }
  }
}

object ZooKeeperEngineSecuritySecretProviderImpl {
  private val SECRET_CHARS = (('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9')).toArray

  private[authentication] def generateSecret(conf: KyuubiConf): String = {
    val length = conf.get(ENGINE_SECURITY_CRYPTO_KEY_LENGTH) / java.lang.Byte.SIZE
    val random = new SecureRandom()
    (0 until length).map(_ => SECRET_CHARS(random.nextInt(SECRET_CHARS.length))).mkString
  }
}
