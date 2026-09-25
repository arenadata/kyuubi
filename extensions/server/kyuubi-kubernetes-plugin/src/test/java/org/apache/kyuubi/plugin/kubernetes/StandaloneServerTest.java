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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.ha.client.ServiceNodeInfo;
import org.apache.kyuubi.server.KyuubiServer;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * A Kyuubi server with nothing but this plugin behind it: no ZooKeeper, embedded or otherwise. A
 * session opens, the server starts an engine in a process of its own, the engine registers through
 * {@link PodDiscoveryClient}, the server finds it there and runs the statement, and once the engine
 * has been idle it stops and its registration goes with it.
 *
 * <p>The engine is the chat engine with its echo provider, because it needs nothing outside the
 * pod. It is found the way a packaged server finds one, under {@code KYUUBI_HOME}, which is the
 * repository root here: the test is skipped when the engine has not been built.
 */
public class StandaloneServerTest {

  @ClassRule public static final Timeout WHOLE_CLASS = Timeout.seconds(600);

  private static final Path REPO =
      Paths.get("").toAbsolutePath().getParent().getParent().getParent();
  private static Path registry;
  private static KyuubiServer server;

  @BeforeClass
  public static void startServer() throws Exception {
    Path engineTarget = REPO.resolve("externals/kyuubi-chat-engine/target");
    boolean engineBuilt;
    try (Stream<Path> files =
        Files.exists(engineTarget) ? Files.list(engineTarget) : Stream.empty()) {
      engineBuilt =
          files.anyMatch(f -> f.getFileName().toString().matches("kyuubi-chat-engine_.*\\.jar"));
    }
    Assume.assumeTrue("the chat engine is not built: " + engineTarget, engineBuilt);

    registry = Files.createTempDirectory("kyuubi-discovery");
    Path work = Files.createTempDirectory("kyuubi-work");
    Path pluginClasses = Paths.get("target/scala-2.13/classes").toAbsolutePath();
    if (!Files.isDirectory(pluginClasses)) {
      pluginClasses = Paths.get("target/scala-2.12/classes").toAbsolutePath();
    }

    KyuubiConf conf = new KyuubiConf(false);
    conf.set("kyuubi.frontend.protocols", "THRIFT_BINARY");
    conf.set("kyuubi.frontend.thrift.binary.bind.port", "0");
    // The plugin in place of ZooKeeper. The address is any value: empty starts an embedded one.
    conf.set("kyuubi.ha.addresses", "pod");
    conf.set("kyuubi.ha.client.class", PodDiscoveryClient.class.getName());
    conf.set(PodDiscoveryClient.DIR_KEY, registry.toString());
    // An engine the pod can run on its own, found where a packaged server would find it.
    conf.set("kyuubi.engine.type", "CHAT");
    conf.set("kyuubi.engine.chat.provider", "ECHO");
    conf.set("kyuubi.engine.share.level", "USER");
    conf.set("kyuubi.engineEnv.KYUUBI_HOME", REPO.toString());
    conf.set("kyuubi.engineEnv.KYUUBI_WORK_DIR_ROOT", work.toString());
    // The engine is a separate JVM and registers through the plugin too.
    conf.set("kyuubi.engine.chat.extra.classpath", pluginClasses.toString());
    conf.set("kyuubi.engine.chat.memory", "256m");
    // Short, so the test sees the engine go away on its own.
    conf.set("kyuubi.session.engine.idle.timeout", "PT5S");
    conf.set("kyuubi.session.engine.check.interval", "PT1S");
    conf.set("kyuubi.operation.log.dir.root", work.resolve("operation_logs").toString());
    conf.set("kyuubi.metrics.enabled", "false");

    server = KyuubiServer.startServer(conf);
  }

  @AfterClass
  public static void stopServer() {
    if (server != null) {
      server.stop();
    }
  }

  private static String jdbcUrl(String user) {
    return "jdbc:kyuubi://" + server.frontendServices().head().connectionUrl() + "/;user=" + user;
  }

  private static PodDiscoveryClient registryClient() {
    KyuubiConf conf = new KyuubiConf(false);
    conf.set(PodDiscoveryClient.DIR_KEY, registry.toString());
    PodDiscoveryClient client = new PodDiscoveryClient(conf);
    client.createClient();
    return client;
  }

  /** The discovery namespace of alice's chat engine, once anything is registered under it. */
  private static Optional<String> engineNamespace() throws Exception {
    try (Stream<Path> dirs = Files.walk(registry)) {
      return dirs.filter(Files::isDirectory)
          .map(d -> "/" + registry.relativize(d).toString().replace(File.separatorChar, '/'))
          .filter(p -> p.contains("_USER_CHAT/alice/") && !p.contains("serverUri="))
          .filter(p -> p.endsWith("/default"))
          .findFirst();
    }
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    for (int i = 0; i < 600; i++) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    fail(what);
  }

  @Test
  public void aSessionRunsOnAnEngineRegisteredThroughThePod() throws Exception {
    Class.forName("org.apache.kyuubi.jdbc.KyuubiHiveDriver");
    String answer;
    try (Connection connection = DriverManager.getConnection(jdbcUrl("alice"));
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("hello from a pod")) {
      assertTrue(rows.next());
      answer = rows.getString(1);
    }
    assertFalse("the echo engine answered", answer == null || answer.isEmpty());

    // The engine, a separate process, is in the registry under the path the server looked in.
    String namespace =
        engineNamespace().orElseThrow(() -> new AssertionError("no engine registered"));
    PodDiscoveryClient client = registryClient();
    List<ServiceNodeInfo> engines = client.nodesIn(namespace, Optional.empty(), false);
    assertEquals(1, engines.size());
    assertTrue(client.getServerHost(namespace).isDefined());
    assertEquals(engines.get(0).port(), client.getServerHost(namespace).get()._2());

    // A second session of the same user finds that engine rather than starting another.
    try (Connection connection = DriverManager.getConnection(jdbcUrl("alice"));
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("again")) {
      assertTrue(rows.next());
    }
    assertEquals(1, client.nodesIn(namespace, Optional.empty(), false).size());

    // Idle, the engine stops itself and takes its registration with it.
    await(
        "the idle engine never left the registry: " + describe(),
        () -> client.nodesIn(namespace, Optional.empty(), true).isEmpty());
    assertTrue(client.getServerHost(namespace).isEmpty());
  }

  private static String describe() throws Exception {
    try (Stream<Path> all = Files.walk(registry)) {
      return all.map(registry::relativize).map(Path::toString).collect(Collectors.joining(", "));
    }
  }
}
