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
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SECURITY_CRYPTO_KEY_LENGTH
import org.apache.kyuubi.ha.HighAvailabilityConf.{
  HA_ADDRESSES,
  HA_ZK_ENGINE_SECURE_SECRET_NODE,
  HA_ZK_ENGINE_SECURE_SECRET_NODE_AUTO_CREATE,
  HA_ZK_NODE_TIMEOUT}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper
import org.apache.kyuubi.zookeeper.ZookeeperConf.ZK_CLIENT_PORT

class ZooKeeperEngineSecuritySecretProviderImplSuite extends KyuubiFunSuite {

  private var zkServer: EmbeddedZookeeper = _
  private var conf: KyuubiConf = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val embeddedZkConf = KyuubiConf()
    embeddedZkConf.set(ZK_CLIENT_PORT, 0)
    zkServer = new EmbeddedZookeeper()
    zkServer.initialize(embeddedZkConf)
    zkServer.start()

    conf = new KyuubiConf()
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_ZK_ENGINE_SECURE_SECRET_NODE, "/kyuubi_engine_secure_secret_test")
  }

  override def afterAll(): Unit = {
    if (zkServer != null) zkServer.stop()
    super.afterAll()
  }

  private def expectedLength: Int =
    conf.get(ENGINE_SECURITY_CRYPTO_KEY_LENGTH) / java.lang.Byte.SIZE

  test("first call creates the secret node and returns a secret of the expected length") {
    val provider = new ZooKeeperEngineSecuritySecretProviderImpl()
    provider.initialize(conf)
    val secret = provider.getSecret()
    assert(secret != null)
    assert(secret.length === expectedLength)
  }

  test("subsequent calls read back the same secret instead of overwriting it") {
    val provider1 = new ZooKeeperEngineSecuritySecretProviderImpl()
    provider1.initialize(conf)
    val first = provider1.getSecret()

    val provider2 = new ZooKeeperEngineSecuritySecretProviderImpl()
    provider2.initialize(conf)
    val second = provider2.getSecret()

    assert(first === second)
  }

  test("concurrent first-time callers all agree on a single generated secret") {
    val concurrentConf = new KyuubiConf()
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_ZK_ENGINE_SECURE_SECRET_NODE, "/kyuubi_engine_secure_secret_concurrent_test")

    val threadCount = 8
    val executor = Executors.newFixedThreadPool(threadCount)
    val results = new CopyOnWriteArrayList[String]()
    try {
      val futures = (0 until threadCount).map { _ =>
        executor.submit(new Runnable {
          override def run(): Unit = {
            val provider = new ZooKeeperEngineSecuritySecretProviderImpl()
            provider.initialize(concurrentConf)
            results.add(provider.getSecret())
          }
        })
      }
      futures.foreach(_.get(30, TimeUnit.SECONDS))
    } finally {
      executor.shutdown()
    }

    val distinctResults = results.asScala.toSet
    assert(results.size() === threadCount)
    assert(
      distinctResults.size === 1,
      s"expected a single agreed-upon secret, got $distinctResults")

    val provider = new ZooKeeperEngineSecuritySecretProviderImpl()
    provider.initialize(concurrentConf)
    assert(provider.getSecret() === distinctResults.head)
  }

  test("regression: a reader never observes the node between create() and setData()") {
    // Simulates a slow writer that holds the same lock getSecret() uses internally, and has
    // created the (still-empty) node but not yet written the real secret to it. Before the fix,
    // getSecret()'s initial pathExists()/getData() fast path ran outside that lock, so a
    // concurrent reader could see the node mid-creation and return the empty placeholder as the
    // "secret" instead of blocking until the real secret was written.
    val zkNode = "/kyuubi_engine_secure_secret_race_test"
    val raceConf = new KyuubiConf()
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_ZK_ENGINE_SECURE_SECRET_NODE, zkNode)

    val realSecret = "R" * expectedLength
    val nodeCreatedLatch = new CountDownLatch(1)
    val releaseWriterLatch = new CountDownLatch(1)

    val writer = new Thread(() => {
      withDiscoveryClient(raceConf) { discoveryClient =>
        discoveryClient.tryWithLock(s"${zkNode}_lock", raceConf.get(HA_ZK_NODE_TIMEOUT)) {
          discoveryClient.create(zkNode, "PERSISTENT")
          nodeCreatedLatch.countDown()
          // Hold the lock - and leave the node empty - until the reader has had a chance to run.
          assert(releaseWriterLatch.await(10, TimeUnit.SECONDS), "test writer was not released")
          discoveryClient.setData(zkNode, realSecret.getBytes(StandardCharsets.UTF_8))
        }
      }
    })
    writer.start()
    assert(nodeCreatedLatch.await(10, TimeUnit.SECONDS), "node was never created")

    val readerResult = new AtomicReference[String]()
    val reader = new Thread(() => {
      val provider = new ZooKeeperEngineSecuritySecretProviderImpl()
      provider.initialize(raceConf)
      readerResult.set(provider.getSecret())
    })
    reader.start()

    // Give the reader a chance to run while the writer is still mid-creation; a correct
    // implementation blocks it on the lock instead of letting it read the empty node.
    Thread.sleep(1000)
    assert(readerResult.get() == null, "reader must not observe the node before setData()")

    releaseWriterLatch.countDown()
    writer.join(10000)
    reader.join(10000)

    assert(readerResult.get() === realSecret)
  }

  test("auto-create disabled and node missing throws instead of creating it") {
    val disabledConf = new KyuubiConf()
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_ZK_ENGINE_SECURE_SECRET_NODE, "/kyuubi_engine_secure_secret_disabled_test")
      .set(HA_ZK_ENGINE_SECURE_SECRET_NODE_AUTO_CREATE, false)
    val provider = new ZooKeeperEngineSecuritySecretProviderImpl()
    provider.initialize(disabledConf)

    val e = intercept[IllegalArgumentException](provider.getSecret())
    assert(e.getMessage.contains("does not exist"))
    assert(e.getMessage.contains(HA_ZK_ENGINE_SECURE_SECRET_NODE_AUTO_CREATE.key))
  }

  test("auto-create disabled but node already provisioned reads it normally") {
    val zkNode = "/kyuubi_engine_secure_secret_preprovisioned_test"
    val provisioningConf = new KyuubiConf()
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_ZK_ENGINE_SECURE_SECRET_NODE, zkNode)
    val provisioningProvider = new ZooKeeperEngineSecuritySecretProviderImpl()
    provisioningProvider.initialize(provisioningConf)
    provisioningProvider.getSecret()

    val disabledConf = new KyuubiConf()
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_ZK_ENGINE_SECURE_SECRET_NODE, zkNode)
      .set(HA_ZK_ENGINE_SECURE_SECRET_NODE_AUTO_CREATE, false)
    val provider = new ZooKeeperEngineSecuritySecretProviderImpl()
    provider.initialize(disabledConf)

    assert(provider.getSecret() != null)
  }
}
