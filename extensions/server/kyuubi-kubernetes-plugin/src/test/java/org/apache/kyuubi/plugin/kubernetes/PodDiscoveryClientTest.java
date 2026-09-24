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

package org.apache.kyuubi.plugin.kubernetes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kyuubi.KyuubiSQLException;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.ha.client.ServiceNodeInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.Tuple2;

public class PodDiscoveryClientTest {

  private static final String NS = "/kyuubi_1.11_USER_TRINO/alice/default";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private PodDiscoveryClient client() {
    return client(new KyuubiConf(false));
  }

  /** Another process on the same pod: a separate client over the same directory. */
  private PodDiscoveryClient client(KyuubiConf conf) {
    conf.set(PodDiscoveryClient.DIR_KEY, tmp.getRoot().getAbsolutePath());
    PodDiscoveryClient c = new PodDiscoveryClient(conf);
    c.createClient();
    return c;
  }

  private static String register(PodDiscoveryClient c, String instance, Optional<String> refId) {
    return c.createServiceNode(NS, instance, "1.11.1", Map.of(), refId, false);
  }

  @Test
  public void anEngineRegisteredByOneProcessIsFoundByAnother() {
    PodDiscoveryClient engine = client();
    PodDiscoveryClient server = client();
    String node = register(engine, "10.0.0.7:10009", Optional.of("ref-1"));
    assertTrue(
        node,
        node.startsWith(NS + "/serverUri=10.0.0.7:10009;version=1.11.1;refId=ref-1;sequence="));

    Option<Tuple2<String, Object>> host = server.getServerHost(NS);
    assertTrue(host.isDefined());
    assertEquals("10.0.0.7", host.get()._1());
    assertEquals(10009, host.get()._2());

    ServiceNodeInfo info = server.nodesIn(NS, Optional.empty(), false).get(0);
    assertEquals("1.11.1", info.version().get());
    assertEquals("ref-1", info.engineRefId().get());
    assertEquals(10009, server.getEngineByRefId(NS, "ref-1").get()._2());
    assertTrue(server.getEngineByRefId(NS, "other").isEmpty());
  }

  @Test
  public void theLatestRegistrationIsTheServerHost() {
    PodDiscoveryClient c = client();
    register(c, "10.0.0.1:1", Optional.empty());
    register(c, "10.0.0.2:2", Optional.empty());
    assertEquals("10.0.0.2", c.getServerHost(NS).get()._1());
    List<ServiceNodeInfo> all = c.nodesIn(NS, Optional.empty(), false);
    assertEquals(List.of("10.0.0.1", "10.0.0.2"), all.stream().map(ServiceNodeInfo::host).toList());
  }

  @Test
  public void aNodeWhoseProcessIsGoneIsDropped() throws Exception {
    PodDiscoveryClient c = client();
    String node = register(c, "10.0.0.9:9", Optional.empty());
    // A process that has certainly exited, with its start time so a reused pid cannot pass for it.
    Process gone = new ProcessBuilder("true").start();
    long pid = gone.pid();
    long started = gone.info().startInstant().map(i -> i.toEpochMilli()).orElse(0L);
    gone.waitFor();
    Files.write(
        c.resolve(node).resolve(PodDiscoveryClient.OWNER),
        (pid + " " + started).getBytes(StandardCharsets.UTF_8));

    assertTrue(c.getServerHost(NS).isEmpty());
    assertFalse("the stale node is removed, not just skipped", c.pathExists(node));
  }

  @Test
  public void deletingTheOwnNodeStopsItsOwner() throws Exception {
    PodDiscoveryClient engine = client();
    PodDiscoveryClient server = client();
    String node = register(engine, "10.0.0.7:10009", Optional.empty());
    CountDownLatch stopped = new CountDownLatch(1);
    engine.track(node, stopped::countDown);

    server.delete(node, false);
    assertTrue("the engine is told its registration is gone", stopped.await(15, TimeUnit.SECONDS));
  }

  @Test
  public void deregisteringAlsoEndsInTheWatchersCallback() throws Exception {
    // What the server's own stop sequence waits for: under ZooKeeper, deleting the node fires
    // the watcher, and the graceful stop it triggers is what lets stop() finish.
    PodDiscoveryClient engine = client();
    String node = register(engine, "10.0.0.7:10009", Optional.empty());
    CountDownLatch stopped = new CountDownLatch(1);
    engine.track(node, stopped::countDown);
    engine.deregisterService();
    assertTrue(stopped.await(15, TimeUnit.SECONDS));
    assertFalse(engine.pathExists(node));
  }

  @Test
  public void aServiceNodeCarriesTheEngineRefIdOfItsConfiguration() {
    KyuubiConf conf = new KyuubiConf(false);
    conf.set("kyuubi.ha.engine.ref.id", "ref-2");
    PodDiscoveryClient engine = client(conf);
    String node = engine.createAndGetServiceNode(conf, NS, "h:1", Option.empty(), false);
    assertTrue(engine.pathExists(node));
    assertTrue(node, node.contains("refId=ref-2"));
    assertEquals("h", engine.getEngineByRefId(NS, "ref-2").get()._1());
  }

  @Test
  public void locksExcludeOtherProcessesAndTimeOut() throws Exception {
    PodDiscoveryClient a = client();
    PodDiscoveryClient b = client();
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder =
        new Thread(
            () ->
                a.tryWithLock(
                    "/locks/alice",
                    1000,
                    () -> {
                      held.countDown();
                      try {
                        release.await();
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      return null;
                    }));
    holder.start();
    assertTrue(held.await(5, TimeUnit.SECONDS));

    KyuubiSQLException timeout =
        assertThrows(
            KyuubiSQLException.class, () -> b.tryWithLock("/locks/alice", 300, () -> "no"));
    assertTrue(
        timeout.getMessage(),
        timeout.getMessage().contains("Timeout to lock on path [/locks/alice]"));

    release.countDown();
    holder.join();
    assertEquals("yes", b.tryWithLock("/locks/alice", 1000, () -> "yes"));
  }

  @Test
  public void aLockIsReentrantWithinAThread() {
    PodDiscoveryClient a = client();
    String out =
        a.tryWithLock("/locks/x", 1000, () -> a.tryWithLock("/locks/x", 1000, () -> "nested"));
    assertEquals("nested", out);
  }

  @Test
  public void theCounterCountsAcrossProcesses() {
    PodDiscoveryClient a = client();
    PodDiscoveryClient b = client();
    assertEquals(0, a.getAndIncrement("/seq/pool", 1));
    assertEquals(1, b.getAndIncrement("/seq/pool", 1));
    assertEquals(2, a.getAndIncrement("/seq/pool", 3));
    assertEquals(5, b.getAndIncrement("/seq/pool", 1));
  }

  @Test
  public void sequentialNodesAreOrderedAndNeverCollide() {
    PodDiscoveryClient c = client();
    String first = c.create("/q/node-", "PERSISTENT_SEQUENTIAL", true);
    String second = c.create("/q/node-", "PERSISTENT_SEQUENTIAL", true);
    assertEquals("/q/node-0000000000", first);
    assertEquals("/q/node-0000000001", second);
    assertEquals(
        List.of("node-0000000000", "node-0000000001"),
        scala.collection.JavaConverters.seqAsJavaList(c.getChildren("/q")));
    assertThrows(
        IllegalStateException.class, () -> c.create("/q/node-0000000000", "PERSISTENT", true));
  }

  @Test
  public void prefixesAndChildrenBehaveLikeTheZooKeeperClient() {
    PodDiscoveryClient c = client();
    c.create("/a/b/c-1", "PERSISTENT", true);
    c.setData("/a/b/c-1", "payload".getBytes(StandardCharsets.UTF_8));
    assertEquals("payload", new String(c.getData("/a/b/c-1"), StandardCharsets.UTF_8));
    assertFalse(c.pathNonExists("/a/b/c", true));
    assertTrue(c.pathNonExists("/a/b/d", true));
    assertTrue(c.pathNonExists("/a/b/c", false));

    assertThrows(IllegalStateException.class, () -> c.delete("/a/b", false));
    c.delete("/a/b", true);
    assertFalse(c.pathExists("/a/b"));
    c.delete("/a/b", true);
  }

  @Test
  public void aNamespaceIsCleanedUpAfterItsLastEngine() {
    PodDiscoveryClient c = client();
    register(c, "h:1", Optional.empty());
    assertTrue(c.postDeregisterService(NS));
    assertFalse(c.pathExists(NS));
    assertFalse(c.postDeregisterService(null));
  }

  @Test
  public void aSecretNodeIsCreatedOnceAndThenAdopted() {
    PodDiscoveryClient c = client();
    c.startSecretNode("PERSISTENT", "/secret/engine", "first", false);
    c.startSecretNode("PERSISTENT", "/secret/engine", "second", false);
    assertEquals("first", new String(c.getData("/secret/engine"), StandardCharsets.UTF_8));
  }

  @Test
  public void theRegistryStaysInsideItsRoot() {
    PodDiscoveryClient c = client();
    assertThrows(IllegalArgumentException.class, () -> c.pathExists("/../outside"));
    Path root = new File(tmp.getRoot(), "").toPath();
    assertTrue(c.resolve("/x/y").startsWith(root));
  }
}
