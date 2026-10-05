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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.kyuubi.Logging;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.ha.client.DiscoveryClient;
import org.apache.kyuubi.ha.client.ServiceDiscovery;
import org.apache.kyuubi.ha.client.ServiceNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * Service discovery for a Kyuubi server whose engines are all started by somebody else.
 *
 * <p>Engines run as workloads of their own - a Spark Thrift Server, a Kyuubi engine an operator
 * keeps up - behind a Service. The Service that declares an engine profile also says where its
 * engine is, with a port named {@value ServiceProfiles#ENGINE_PORT_NAME}, and that is what a
 * session of the profile is connected to. The answer comes from the informer {@link
 * ClusterProfileAdvisor} already keeps; nothing is asked of the api server per session, and nothing
 * is written anywhere.
 *
 * <p>Kyuubi never starts an engine with this client. The server asks for an engine before it would
 * start one, and an engine space no Service serves is answered with a {@code KyuubiSQLException}
 * naming the reason, which fails the session instead of launching a process.
 *
 * <p>The server's own registration goes nowhere: clients reach servers through their Service. The
 * rest of the interface - paths, locks, counters, used by engine pools and {@code kyuubi-ctl} - is
 * kept in this process's memory.
 */
public class KubernetesDiscoveryClient implements DiscoveryClient {

  static final String ENGINE_TYPE_KEY = "kyuubi.engine.type";

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesDiscoveryClient.class);

  // Per JVM, not per client: the server creates a client for every lookup and closes it after.
  private static final Map<String, byte[]> NODES = new ConcurrentSkipListMap<>();
  private static final Map<String, AtomicInteger> COUNTERS = new ConcurrentHashMap<>();
  private static final Map<String, ReentrantLock> LOCKS = new ConcurrentHashMap<>();
  private static final AtomicLong SEQUENCE = new AtomicLong();

  private final KyuubiConf conf;
  private volatile ClusterCatalog catalog;
  private volatile ServiceDiscovery registered;

  // The state of the Logging trait DiscoveryClient extends; Scala would have generated these.
  private transient Logger log_;

  public KubernetesDiscoveryClient(KyuubiConf conf) {
    Logging.$init$(this);
    this.conf = conf;
  }

  /** For tests. */
  KubernetesDiscoveryClient(KyuubiConf conf, ClusterCatalog catalog) {
    this(conf);
    this.catalog = catalog;
  }

  @Override
  public Logger org$apache$kyuubi$Logging$$log_() {
    return log_;
  }

  @Override
  public void org$apache$kyuubi$Logging$$log__$eq(Logger logger) {
    this.log_ = logger;
  }

  @Override
  public void createClient() {
    catalog();
  }

  /** The catalog is shared and outlives every client; there is nothing to close. */
  @Override
  public void closeClient() {}

  private ClusterCatalog catalog() {
    ClusterCatalog c = catalog;
    if (c == null) {
      c = SharedCatalog.get(conf);
      catalog = c;
    }
    return c;
  }

  // ---------------------------------------------------------------- engines

  /**
   * The engine of an engine space, from the Service that declares it.
   *
   * @throws org.apache.kyuubi.KyuubiSQLException when no Service runs an engine for the space -
   *     thrown rather than answered with nothing, because nothing makes the server start one
   */
  @Override
  public Option<Tuple2<String, Object>> getServerHost(String engineSpace) {
    ClusterProfile.Engine engine = engineFor(engineSpace);
    return Option.apply(new Tuple2<>(engine.host(), engine.port()));
  }

  /** There is one engine per profile, whatever the reference it was asked for under. */
  @Override
  public Option<Tuple2<String, Object>> getEngineByRefId(String engineSpace, String engineRefId) {
    return getServerHost(engineSpace);
  }

  @Override
  public scala.collection.immutable.Seq<ServiceNodeInfo> getServiceNodesInfo(
      String engineSpace, Option<Object> sizeOpt, boolean silent) {
    List<ServiceNodeInfo> nodes = new ArrayList<>();
    Optional<ClusterProfile> profile = profileFor(engineSpace);
    if (profile.isPresent() && profile.get().engine().isPresent()) {
      ClusterProfile.Engine engine = profile.get().engine().get();
      scala.collection.immutable.Map<String, String> attributes =
          scala.collection.immutable.Map$.MODULE$
              .<String, String>empty()
              .updated("service", profile.get().source());
      nodes.add(
          new ServiceNodeInfo(
              engineSpace,
              "service=" + profile.get().source(),
              engine.host(),
              engine.port(),
              Option.empty(),
              Option.empty(),
              attributes));
    }
    return JavaConverters.asScalaBuffer(nodes).toList();
  }

  ClusterProfile.Engine engineFor(String engineSpace) {
    Optional<ClusterProfile> profile = profileFor(engineSpace);
    if (!profile.isPresent()) {
      throw Unchecked.sqlException(
          "No engine runs for engine space "
              + engineSpace
              + ": no Service declares a profile for it. Engines are started by operators,"
              + " not by Kyuubi; profiles from Services: "
              + catalog().profiles().stream()
                  .map(ClusterProfile::toString)
                  .collect(Collectors.toList())
              + ".",
          null);
    }
    if (!profile.get().engine().isPresent()) {
      throw Unchecked.sqlException(
          "No engine runs for profile '"
              + profile.get().name()
              + "': Service "
              + profile.get().source()
              + " has no port named '"
              + ServiceProfiles.ENGINE_PORT_NAME
              + "'. Engines are started by operators, not by Kyuubi.",
          null);
    }
    return profile.get().engine().get();
  }

  /**
   * The profile an engine space belongs to: the one of its engine type whose subdomain the space
   * ends with.
   *
   * <p>A space is {@code /<namespace>_<version>_<share level>_<engine type>/<user>/<subdomain>},
   * with {@code <host>_<subdomain>} as its last part at {@code SERVER_LOCAL}. The advisor gives
   * every profile its own subdomain, so the last part names the profile; at {@code CONNECTION}
   * share level it is a reference id instead, and nothing is found - a shared engine serves no
   * single connection.
   */
  Optional<ClusterProfile> profileFor(String engineSpace) {
    String[] parts =
        java.util.Arrays.stream(engineSpace.split("/"))
            .filter(p -> !p.isEmpty())
            .toArray(String[]::new);
    if (parts.length < 3) {
      return Optional.empty();
    }
    String root = parts[0].toUpperCase(Locale.ROOT);
    String last = parts[parts.length - 1];
    return catalog().profiles().stream()
        .filter(
            p -> {
              String type = p.conf().getOrDefault(ENGINE_TYPE_KEY, "");
              return !type.isEmpty() && root.endsWith("_" + type.toUpperCase(Locale.ROOT));
            })
        .filter(
            p -> {
              String subdomain = p.conf().get(ServiceProfiles.SUBDOMAIN_KEY);
              return subdomain != null
                  && (last.equals(subdomain) || last.endsWith("_" + subdomain));
            })
        .findFirst();
  }

  // ---------------------------------------------------------------- the server itself

  /** Not registered anywhere: clients reach servers through their Service. */
  @Override
  public void registerService(
      KyuubiConf conf,
      String namespace,
      ServiceDiscovery serviceDiscovery,
      Option<String> version,
      boolean external) {
    registered = serviceDiscovery;
    LOG.info(
        "{} is reached through its Service; nothing to register in {}",
        serviceDiscovery.fe().connectionUrl(),
        namespace);
  }

  /**
   * Starts the graceful stop the server waits for.
   *
   * <p>Under ZooKeeper, deregistering deletes the node and the watcher on it starts the graceful
   * stop, which {@code KyuubiServiceDiscovery.stop} waits for. Nothing watches here, so it is
   * started directly, from a thread of its own as the watcher's would be.
   */
  @Override
  public void deregisterService() {
    ServiceDiscovery discovery = registered;
    registered = null;
    if (discovery != null) {
      Thread stop = new Thread(() -> discovery.stopGracefully(false), "kubernetes-discovery-stop");
      stop.setDaemon(true);
      stop.start();
    }
  }

  @Override
  public boolean postDeregisterService(String namespace) {
    return namespace != null;
  }

  /** Nothing to monitor: there is no connection to lose. */
  @Override
  public void monitorState(ServiceDiscovery serviceDiscovery) {}

  // ---------------------------------------------------------------- the rest, in memory

  @Override
  public String createAndGetServiceNode(
      KyuubiConf conf,
      String namespace,
      String instance,
      Option<String> version,
      boolean external) {
    String path =
        create(namespace + "/serverUri=" + instance + ";sequence=", "EPHEMERAL_SEQUENTIAL", true);
    setData(path, instance.getBytes(StandardCharsets.UTF_8));
    return path;
  }

  @Override
  public String create(String path, String mode, boolean createParent) {
    String node = normalize(path);
    if (mode.toUpperCase(Locale.ROOT).endsWith("SEQUENTIAL")) {
      node = node + String.format("%010d", SEQUENCE.getAndIncrement());
    } else if (NODES.containsKey(node)) {
      throw new IllegalStateException(path + " already exists");
    }
    NODES.put(node, new byte[0]);
    return node;
  }

  @Override
  public byte[] getData(String path) {
    byte[] data = NODES.get(normalize(path));
    return data == null ? new byte[0] : data;
  }

  @Override
  public boolean setData(String path, byte[] data) {
    String node = normalize(path);
    if (!NODES.containsKey(node)) {
      throw new IllegalStateException("No such node " + path);
    }
    NODES.put(node, data);
    return true;
  }

  @Override
  public scala.collection.immutable.List<String> getChildren(String path) {
    return JavaConverters.asScalaBuffer(children(normalize(path))).toList();
  }

  @Override
  public boolean pathExists(String path) {
    String node = normalize(path);
    return NODES.containsKey(node) || !children(node).isEmpty();
  }

  @Override
  public boolean pathNonExists(String path, boolean isPrefix) {
    String node = normalize(path);
    if (!isPrefix) {
      return !pathExists(node);
    }
    return NODES.keySet().stream().noneMatch(k -> k.startsWith(node));
  }

  /** Service nodes are not this client's to delete; anything else it holds is. */
  @Override
  public void delete(String path, boolean deleteChildren) {
    String node = normalize(path);
    if (node.contains("/service=")) {
      LOG.info("{} is an engine behind a Service and stays; its operator owns it", node);
      return;
    }
    if (!deleteChildren && !children(node).isEmpty()) {
      throw new IllegalStateException(path + " has children");
    }
    NODES.keySet().removeIf(k -> k.equals(node) || k.startsWith(node + "/"));
  }

  @Override
  public <T> T tryWithLock(String lockPath, long timeout, Function0<T> f) {
    ReentrantLock lock = LOCKS.computeIfAbsent(normalize(lockPath), k -> new ReentrantLock());
    boolean held;
    try {
      held = lock.tryLock(Math.max(timeout, 0L), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Unchecked.sqlException("Interrupted locking path [" + lockPath + "]", e);
    }
    if (!held) {
      throw Unchecked.sqlException(
          "Timeout to lock on path [" + lockPath + "] after " + timeout + " ms.", null);
    }
    try {
      return f.apply();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int getAndIncrement(String path, int delta) {
    return COUNTERS.computeIfAbsent(normalize(path), k -> new AtomicInteger()).getAndAdd(delta);
  }

  @Override
  public void startSecretNode(
      String createMode, String basePath, String initData, boolean useProtection) {
    NODES.putIfAbsent(normalize(basePath), initData.getBytes(StandardCharsets.UTF_8));
  }

  private static List<String> children(String node) {
    String prefix = node.endsWith("/") ? node : node + "/";
    return NODES.keySet().stream()
        .filter(k -> k.startsWith(prefix))
        .map(k -> k.substring(prefix.length()).split("/", 2)[0])
        .distinct()
        .collect(Collectors.toList());
  }

  private static String normalize(String path) {
    String p = path.startsWith("/") ? path : "/" + path;
    return p.length() > 1 && p.endsWith("/") ? p.substring(0, p.length() - 1) : p;
  }

  /** For tests: forget everything held in memory. */
  static void reset() {
    NODES.clear();
    COUNTERS.clear();
    LOCKS.clear();
  }
}
