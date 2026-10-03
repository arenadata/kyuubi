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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.server.KyuubiServer;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * A Kyuubi server that starts no engine and runs no ZooKeeper.
 *
 * <p>An engine is started here the way an operator would start one - a process of its own, on a
 * port of its own, registered nowhere - and declared as a profile whose Service names that port. A
 * session asking for the profile runs on it; a session asking for a profile with no engine is
 * refused, and no engine process is started for it.
 *
 * <p>The engine is the chat engine with its echo provider, because it needs nothing else. The test
 * is skipped when it has not been built.
 */
public class StandaloneServerTest {

  @ClassRule public static final Timeout WHOLE_CLASS = Timeout.seconds(300);

  private static final Path REPO =
      Paths.get("").toAbsolutePath().getParent().getParent().getParent();
  private static Process engine;
  private static KyuubiServer server;

  @BeforeClass
  public static void start() throws Exception {
    Path target = REPO.resolve("externals/kyuubi-chat-engine/target");
    Optional<Path> jar;
    try (Stream<Path> files = Files.exists(target) ? Files.list(target) : Stream.empty()) {
      jar =
          files
              .filter(
                  f ->
                      f.getFileName()
                          .toString()
                          .matches("kyuubi-chat-engine_[0-9.]+-.*[0-9]\\.jar"))
              .findFirst();
    }
    Path deps = target.resolve("scala-2.13/jars");
    Assume.assumeTrue(
        "the chat engine is not built: " + target, jar.isPresent() && Files.isDirectory(deps));

    int port;
    try (ServerSocket free = new ServerSocket(0)) {
      port = free.getLocalPort();
    }
    Path work = Files.createTempDirectory("kyuubi-engine");
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-Xmx256m");
    command.add("-cp");
    command.add(jar.get() + File.pathSeparator + deps + File.separator + "*");
    command.add("org.apache.kyuubi.engine.chat.ChatEngine");
    for (String kv :
        new String[] {
          "kyuubi.frontend.thrift.binary.bind.host=127.0.0.1",
          "kyuubi.frontend.thrift.binary.bind.port=" + port,
          "kyuubi.engine.chat.provider=ECHO",
          "kyuubi.engine.share.level=SERVER",
          // Kept up between sessions, as an engine an operator runs is.
          "kyuubi.session.engine.idle.timeout=PT0S",
          "kyuubi.session.user=kyuubi",
          "kyuubi.operation.log.dir.root=" + work.resolve("logs")
        }) {
      command.add("--conf");
      command.add(kv);
    }
    engine =
        new ProcessBuilder(command)
            .directory(work.toFile())
            .redirectErrorStream(true)
            .redirectOutput(work.resolve("engine.out").toFile())
            .start();
    awaitPort(port);

    // The Service that would declare it; an informer would fill the same catalog.
    SharedCatalog.set(
        () ->
            List.of(
                new ClusterProfile(
                    "echo",
                    "engines/echo",
                    Set.of(),
                    true,
                    Map.of(
                        "kyuubi.engine.type", "CHAT",
                        "kyuubi.engine.share.level.subdomain", "echo"),
                    Optional.of(new ClusterProfile.Engine("127.0.0.1", port))),
                new ClusterProfile(
                    "stopped",
                    "engines/stopped",
                    Set.of(),
                    false,
                    Map.of(
                        "kyuubi.engine.type", "CHAT",
                        "kyuubi.engine.share.level.subdomain", "stopped"),
                    Optional.empty())));

    KyuubiConf conf = new KyuubiConf(false);
    conf.set("kyuubi.frontend.protocols", "THRIFT_BINARY");
    conf.set("kyuubi.frontend.thrift.binary.bind.port", "0");
    conf.set("kyuubi.ha.addresses", "kubernetes");
    conf.set("kyuubi.ha.client.class", KubernetesDiscoveryClient.class.getName());
    conf.set("kyuubi.session.conf.advisor", ClusterProfileAdvisor.class.getName());
    conf.set("kyuubi.engine.profiles.unknown.strategy", "LOG");
    conf.set("kyuubi.engine.share.level", "SERVER");
    conf.set("kyuubi.session.engine.launch.async", "false");
    conf.set("kyuubi.operation.log.dir.root", work.resolve("server-logs").toString());
    conf.set("kyuubi.metrics.enabled", "false");
    server = KyuubiServer.startServer(conf);
  }

  @AfterClass
  public static void stop() {
    if (server != null) {
      server.stop();
    }
    if (engine != null) {
      engine.destroy();
    }
    SharedCatalog.set(null);
  }

  private static void awaitPort(int port) throws Exception {
    for (int i = 0; i < 600; i++) {
      try (Socket s = new Socket("127.0.0.1", port)) {
        return;
      } catch (java.io.IOException e) {
        if (!engine.isAlive()) {
          throw new IllegalStateException("the engine exited: " + engine.exitValue());
        }
        Thread.sleep(100);
      }
    }
    throw new IllegalStateException("the engine never opened port " + port);
  }

  private static String url(String profile) {
    return "jdbc:kyuubi://"
        + server.frontendServices().head().connectionUrl()
        + "/;user=alice"
        + (profile == null ? "" : "#kyuubi.engine.profile=" + profile);
  }

  private static String ask(String profile, String sql) throws SQLException {
    try (Connection c = DriverManager.getConnection(url(profile));
        Statement s = c.createStatement();
        ResultSet rows = s.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getString(1);
    }
  }

  @Test
  public void sessionsRunOnTheEngineTheServiceNamesAndNoneIsLaunched() throws Exception {
    Class.forName("org.apache.kyuubi.jdbc.KyuubiHiveDriver");
    String echoed = ask("echo", "hello from an operator's engine");
    assertTrue(echoed, echoed != null && !echoed.isEmpty());
    // The default profile, for a session that asks for none.
    assertTrue(ask(null, "again") != null);

    SQLException refused = null;
    try {
      ask("stopped", "nobody runs this");
    } catch (SQLException e) {
      refused = e;
    }
    assertTrue("a profile with no engine is refused", refused != null);
    assertTrue(refused.getMessage(), refused.getMessage().contains("has no port named 'kyuubi'"));

    long launched =
        ProcessHandle.current().descendants().filter(p -> !p.equals(engine.toHandle())).count();
    assertTrue("the server started no process of its own, it started " + launched, launched == 0);
  }
}
