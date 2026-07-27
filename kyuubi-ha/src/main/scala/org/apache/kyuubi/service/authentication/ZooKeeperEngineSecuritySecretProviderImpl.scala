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
  HA_ZK_ENGINE_SECURE_SECRET_NODE_AUTO_CREATE}
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
    val expectedLength = conf.get(ENGINE_SECURITY_CRYPTO_KEY_LENGTH) / java.lang.Byte.SIZE
    withDiscoveryClient[String](conf) { discoveryClient =>
      // Guards against a node poisoned by an older, pre-atomic-create revision of this class,
      // which could leave non-secret placeholder data (e.g. a host address) in place forever.
      def readValidated(): String = {
        val secret = new String(discoveryClient.getData(zkNode), StandardCharsets.UTF_8)
        if (secret.length != expectedLength) {
          throw new IllegalStateException(
            s"Secret at ZooKeeper node $zkNode has length ${secret.length}, expected " +
              s"$expectedLength; the node is likely corrupted - delete it manually and retry.")
        }
        secret
      }
      if (discoveryClient.pathExists(zkNode)) {
        readValidated()
      } else if (!autoCreate) {
        throw new IllegalArgumentException(
          s"ZooKeeper node $zkNode does not exist and " +
            s"${HA_ZK_ENGINE_SECURE_SECRET_NODE_AUTO_CREATE.key} is false; please create it " +
            "manually before enabling engine security, or leave auto-create enabled.")
      } else {
        // startSecretNode() creates the node with this data atomically - node and data become
        // visible together - so no concurrent reader can ever observe it existing but empty.
        // If another server/engine wins the race, it leaves that node untouched and we just
        // re-read whatever data won below, so no lock is needed here at all.
        discoveryClient.startSecretNode("PERSISTENT", zkNode, generateSecret(conf))
        readValidated()
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
