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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kyuubi.Logging;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.ha.HighAvailabilityConf;
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
 * Service discovery for a Kyuubi pod that stands on its own: a registry on the pod's file system.
 *
 * <p>What Kyuubi keeps in ZooKeeper is where its engines are and a lock around starting one. In
 * Kubernetes the engines a pod launches - Trino, JDBC, Hive - are child processes of that pod, so
 * the only party that needs to find them is the server they belong to, and the file system they
 * share is registry enough. Nothing in it outlives the pod, which is the point: a Kyuubi pod that
 * dies loses its own sessions and nothing else, and no ZooKeeper has to be run for it.
 *
 * <p>Paths are laid out as directories under one root, a node per directory: its data in {@value
 * #DATA}, and for an ephemeral node the process that owns it in {@value #OWNER}. An owner that has
 * exited - by pid and start time, so a pid reused after a container restart is not mistaken for it
 * - makes the node stale, and stale nodes are dropped when the namespace is next read. That is what
 * stands in for ZooKeeper's session: an engine that crashes is forgotten the next time its server
 * looks, and an engine whose node is deleted stops itself, as it would under ZooKeeper.
 *
 * <p>Configured through {@code kyuubi.ha.client.class}; the root is {@value #DIR_KEY}, which the
 * server and every engine it launches read from the same configuration. {@code kyuubi.ha.addresses}
 * has to be set to anything at all, or the server starts an embedded ZooKeeper instead.
 */
public class PodDiscoveryClient implements DiscoveryClient {

  public static final String DIR_KEY = "kyuubi.ha.pod.dir";
  public static final String DEFAULT_DIR =
      Paths.get(System.getProperty("java.io.tmpdir"), "kyuubi-discovery").toString();

  static final String DATA = ".data";
  static final String OWNER = ".owner";
  private static final String LOCK = ".lock";
  private static final String SEQUENCE = ".sequence";
  private static final long WATCH_INTERVAL_MILLIS = 3000L;
  private static final long LOCK_POLL_MILLIS = 50L;

  private static final Logger LOG = LoggerFactory.getLogger(PodDiscoveryClient.class);

  /** Within one JVM a file lock is not reentrant and not shareable; this is what makes it both. */
  private static final Map<Path, ReentrantLock> JVM_LOCKS = new ConcurrentHashMap<>();

  private final Path root;
  private volatile Path ownNode;
  private volatile Thread watcher;

  // The state of the Logging trait DiscoveryClient extends; Scala would have generated these.
  private transient Logger log_;

  public PodDiscoveryClient(KyuubiConf conf) {
    Logging.$init$(this);
    Option<String> dir = conf.getOption(DIR_KEY);
    this.root = Paths.get(dir.isDefined() ? dir.get() : DEFAULT_DIR).toAbsolutePath().normalize();
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
    mkdirs(root);
  }

  /** Closing forgets what this client registered, as a ZooKeeper session ending would. */
  @Override
  public void closeClient() {
    deregisterService();
  }

  @Override
  public String create(String path, String mode, boolean createParent) {
    Path node = resolve(path);
    Path parent = node.getParent();
    if (!Files.isDirectory(parent)) {
      if (!createParent) {
        throw new IllegalStateException("No such parent for " + path);
      }
      mkdirs(parent);
    }
    Mode m = Mode.of(mode);
    if (m.sequential) {
      node = parent.resolve(node.getFileName() + String.format("%010d", nextSequence(parent)));
    } else if (Files.exists(node)) {
      throw new IllegalStateException(path + " already exists");
    }
    mkdirs(node);
    write(node.resolve(DATA), new byte[0]);
    if (m.ephemeral) {
      write(node.resolve(OWNER), Owner.self().toString().getBytes(StandardCharsets.UTF_8));
    }
    return pathOf(node);
  }

  @Override
  public byte[] getData(String path) {
    Path data = resolve(path).resolve(DATA);
    try {
      return Files.exists(data) ? Files.readAllBytes(data) : new byte[0];
    } catch (IOException e) {
      throw new UncheckedIOException("Could not read " + path, e);
    }
  }

  @Override
  public boolean setData(String path, byte[] data) {
    Path node = resolve(path);
    if (!Files.isDirectory(node)) {
      throw new IllegalStateException("No such node " + path);
    }
    write(node.resolve(DATA), data);
    return true;
  }

  @Override
  public scala.collection.immutable.List<String> getChildren(String path) {
    return JavaConverters.asScalaBuffer(children(resolve(path))).toList();
  }

  @Override
  public boolean pathExists(String path) {
    return Files.isDirectory(resolve(path));
  }

  /** With {@code isPrefix}, whether no sibling starts with the last segment of {@code path}. */
  @Override
  public boolean pathNonExists(String path, boolean isPrefix) {
    Path node = resolve(path);
    if (!isPrefix) {
      return !Files.isDirectory(node);
    }
    String prefix = node.getFileName().toString();
    return children(node.getParent()).stream().noneMatch(c -> c.startsWith(prefix));
  }

  @Override
  public void delete(String path, boolean deleteChildren) {
    Path node = resolve(path);
    if (!Files.exists(node)) {
      return;
    }
    if (!deleteChildren && !children(node).isEmpty()) {
      throw new IllegalStateException(path + " has children");
    }
    deleteTree(node);
  }

  /** Nothing to monitor: the file system does not go away on its own. */
  @Override
  public void monitorState(ServiceDiscovery serviceDiscovery) {}

  @Override
  public <T> T tryWithLock(String lockPath, long timeout, Function0<T> f) {
    Path dir = resolve(lockPath);
    mkdirs(dir);
    Path lockFile = dir.resolve(LOCK);
    ReentrantLock jvmLock = JVM_LOCKS.computeIfAbsent(lockFile, k -> new ReentrantLock());
    long deadline = System.currentTimeMillis() + Math.max(timeout, 0L);
    boolean held;
    try {
      held = jvmLock.tryLock(Math.max(timeout, 0L), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Unchecked.sqlException("Interrupted locking path [" + lockPath + "]", e);
    }
    if (!held) {
      throw timedOut(lockPath, timeout);
    }
    try {
      // An outer frame of this thread already holds the file lock; a second lock would overlap.
      if (jvmLock.getHoldCount() > 1) {
        return f.apply();
      }
      try (FileChannel channel =
          FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
        FileLock lock = acquire(channel, deadline, lockPath, timeout);
        try {
          return f.apply();
        } finally {
          lock.release();
        }
      } catch (IOException e) {
        throw Unchecked.sqlException("Lock failed on path [" + lockPath + "]", e);
      }
    } finally {
      jvmLock.unlock();
    }
  }

  private static FileLock acquire(FileChannel channel, long deadline, String lockPath, long timeout)
      throws IOException {
    while (true) {
      try {
        FileLock lock = channel.tryLock();
        if (lock != null) {
          return lock;
        }
      } catch (OverlappingFileLockException e) {
        // Another client in this JVM holds it; it will show up through its own JVM lock next time.
      }
      if (System.currentTimeMillis() >= deadline) {
        throw timedOut(lockPath, timeout);
      }
      try {
        Thread.sleep(LOCK_POLL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Unchecked.sqlException("Interrupted locking path [" + lockPath + "]", e);
      }
    }
  }

  private static RuntimeException timedOut(String lockPath, long timeout) {
    return Unchecked.sqlException(
        "Timeout to lock on path ["
            + lockPath
            + "] after "
            + timeout
            + " ms. There would be some problem that other session may create engine timeout.",
        null);
  }

  @Override
  public Option<Tuple2<String, Object>> getServerHost(String namespace) {
    List<ServiceNodeInfo> nodes = nodesIn(namespace, Optional.of(1), true);
    return nodes.size() == 1 ? Option.apply(hostPort(nodes.get(0))) : Option.empty();
  }

  @Override
  public Option<Tuple2<String, Object>> getEngineByRefId(String namespace, String engineRefId) {
    return Option.apply(
        nodesIn(namespace, Optional.empty(), true).stream()
            .filter(n -> n.engineRefId().isDefined() && n.engineRefId().get().equals(engineRefId))
            .findFirst()
            .map(PodDiscoveryClient::hostPort)
            .orElse(null));
  }

  private static Tuple2<String, Object> hostPort(ServiceNodeInfo node) {
    return new Tuple2<>(node.host(), node.port());
  }

  @Override
  public scala.collection.immutable.Seq<ServiceNodeInfo> getServiceNodesInfo(
      String namespace, Option<Object> sizeOpt, boolean silent) {
    Optional<Integer> size =
        sizeOpt.isDefined() ? Optional.of((Integer) sizeOpt.get()) : Optional.empty();
    return JavaConverters.asScalaBuffer(nodesIn(namespace, size, silent)).toList();
  }

  /** The live nodes of a namespace in registration order, the last {@code size} of them. */
  List<ServiceNodeInfo> nodesIn(String namespace, Optional<Integer> size, boolean silent) {
    try {
      Path dir = resolve(namespace);
      if (!Files.isDirectory(dir)) {
        return List.of();
      }
      List<String> live = new ArrayList<>();
      for (String name : children(dir)) {
        if (isStale(dir.resolve(name))) {
          LOG.info("Dropping {} under {}: the process that registered it is gone", name, namespace);
          deleteTree(dir.resolve(name));
        } else {
          live.add(name);
        }
      }
      live.sort(Comparator.comparingLong(PodDiscoveryClient::sequenceOf).thenComparing(n -> n));
      int keep = Math.min(size.orElse(live.size()), live.size());
      List<ServiceNodeInfo> nodes = new ArrayList<>();
      for (String name : live.subList(live.size() - keep, live.size())) {
        String instance =
            new String(Files.readAllBytes(dir.resolve(name).resolve(DATA)), StandardCharsets.UTF_8);
        Map<String, String> attributes = attributesOf(name);
        HostPort at = HostPort.parse(instance);
        LOG.info(
            "Get service instance:{} and version:{} under {}",
            instance,
            attributes.getOrDefault("version", ""),
            namespace);
        nodes.add(
            new ServiceNodeInfo(
                namespace,
                name,
                at.host,
                at.port,
                Option.apply(attributes.get("version")),
                Option.apply(attributes.get("refId")),
                toScala(attributes)));
      }
      return nodes;
    } catch (IOException | RuntimeException e) {
      if (!silent) {
        LOG.error("Failed to get service node info under " + namespace, e);
      }
      return List.of();
    }
  }

  @Override
  public void registerService(
      KyuubiConf conf,
      String namespace,
      ServiceDiscovery serviceDiscovery,
      Option<String> version,
      boolean external) {
    Map<String, String> attributes =
        new LinkedHashMap<>(JavaConverters.mapAsJavaMap(serviceDiscovery.fe().attributes()));
    String node =
        createServiceNode(
            namespace,
            serviceDiscovery.fe().connectionUrl(),
            versionOr(version),
            attributes,
            refIdOf(conf),
            external);
    track(node, () -> serviceDiscovery.stopGracefully(false));
  }

  /** Makes {@code nodePath} this client's own registration: deregistered by it, and watched. */
  void track(String nodePath, Runnable onGone) {
    ownNode = resolve(nodePath);
    watch(ownNode, onGone);
  }

  /**
   * Removes the registration, and lets the watcher see it go.
   *
   * <p>Under ZooKeeper, deregistering deletes the node and the watcher on it fires, which is how
   * the server's own stop sequence proceeds: {@code KyuubiServiceDiscovery.stop} waits for the
   * graceful stop the watcher triggers. So the watcher is woken to notice, not stopped.
   */
  @Override
  public void deregisterService() {
    Path node = ownNode;
    if (node != null && Files.exists(node)) {
      deleteTree(node);
    }
    Thread w = watcher;
    if (w != null) {
      LockSupport.unpark(w);
    }
  }

  @Override
  public boolean postDeregisterService(String namespace) {
    if (namespace == null) {
      return false;
    }
    delete(namespace, true);
    return true;
  }

  @Override
  public String createAndGetServiceNode(
      KyuubiConf conf,
      String namespace,
      String instance,
      Option<String> version,
      boolean external) {
    return createServiceNode(
        namespace, instance, versionOr(version), Map.of(), refIdOf(conf), external);
  }

  @Override
  public void startSecretNode(
      String createMode, String basePath, String initData, boolean useProtection) {
    if (pathExists(basePath)) {
      LOG.debug("Secret node {} already exists; adopting its existing data.", basePath);
      return;
    }
    create(basePath, createMode, true);
    setData(basePath, initData.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public int getAndIncrement(String path, int delta) {
    return tryWithLock(
        path,
        TimeUnit.MINUTES.toMillis(1),
        () -> {
          Path node = resolve(path);
          Path data = node.resolve(DATA);
          int previous = 0;
          try {
            if (Files.exists(data)) {
              String text = new String(Files.readAllBytes(data), StandardCharsets.UTF_8).trim();
              previous = text.isEmpty() ? 0 : Integer.parseInt(text);
            }
          } catch (IOException e) {
            throw new UncheckedIOException("Could not read the counter at " + path, e);
          }
          write(data, Integer.toString(previous + delta).getBytes(StandardCharsets.UTF_8));
          return previous;
        });
  }

  /**
   * Registers {@code instance} under {@code namespace} the way the ZooKeeper client names its
   * nodes, so that everything reading the name back - the server looking for an engine, {@code
   * kyuubi-ctl} - finds what it expects.
   */
  String createServiceNode(
      String namespace,
      String instance,
      String version,
      Map<String, String> attributes,
      Optional<String> refId,
      boolean external) {
    StringBuilder name = new StringBuilder("serverUri=").append(instance);
    name.append(";version=").append(version);
    attributes.forEach((k, v) -> name.append(';').append(k).append('=').append(v));
    refId.ifPresent(r -> name.append(";refId=").append(r));
    name.append(";sequence=");
    String path = namespace + "/" + name;
    String created =
        create(path, external ? "PERSISTENT_SEQUENTIAL" : "EPHEMERAL_SEQUENTIAL", true);
    setData(created, instance.getBytes(StandardCharsets.UTF_8));
    LOG.info("Registered {} as {}", instance, created);
    return created;
  }

  /**
   * Runs {@code onGone} once {@code node} has been removed by somebody else - the server giving up
   * on an engine that refuses connections, an operator - which is when the process that registered
   * it is meant to stop, as it would when ZooKeeper told it its node was deleted.
   */
  void watch(Path node, Runnable onGone) {
    Thread w =
        new Thread(
            () -> {
              while (!Thread.currentThread().isInterrupted()) {
                if (!Files.isDirectory(node)) {
                  LOG.info("{} is no longer registered, stopping", pathOf(node));
                  ownNode = null;
                  watcher = null;
                  onGone.run();
                  return;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(WATCH_INTERVAL_MILLIS));
              }
            },
            "pod-discovery-watcher");
    w.setDaemon(true);
    watcher = w;
    w.start();
  }

  private static Optional<String> refIdOf(KyuubiConf conf) {
    Option<String> refId = conf.get(HighAvailabilityConf.HA_ENGINE_REF_ID());
    return refId.isDefined() ? Optional.of(refId.get()) : Optional.empty();
  }

  private static String versionOr(Option<String> version) {
    return version.isDefined()
        ? version.get()
        : org.apache.kyuubi.package$.MODULE$.KYUUBI_VERSION();
  }

  /** {@code k=v;k=v} pairs of a node name, as the ZooKeeper client reads them. */
  static Map<String, String> attributesOf(String nodeName) {
    Map<String, String> attributes = new LinkedHashMap<>();
    for (String pair : nodeName.split(";")) {
      String[] kv = pair.split("=", 2);
      if (kv.length == 2) {
        attributes.put(kv[0], kv[1]);
      }
    }
    return attributes;
  }

  private static long sequenceOf(String nodeName) {
    String sequence = attributesOf(nodeName).get("sequence");
    if (sequence == null) {
      return Long.MAX_VALUE;
    }
    try {
      return Long.parseLong(sequence);
    } catch (NumberFormatException e) {
      return Long.MAX_VALUE;
    }
  }

  private static scala.collection.immutable.Map<String, String> toScala(Map<String, String> map) {
    scala.collection.immutable.Map<String, String> result =
        scala.collection.immutable.Map$.MODULE$.empty();
    for (Map.Entry<String, String> e : map.entrySet()) {
      result = result.updated(e.getKey(), e.getValue());
    }
    return result;
  }

  private boolean isStale(Path node) {
    Path owner = node.resolve(OWNER);
    if (!Files.exists(owner)) {
      return false;
    }
    try {
      return !Owner.parse(new String(Files.readAllBytes(owner), StandardCharsets.UTF_8)).isAlive();
    } catch (IOException | RuntimeException e) {
      return true;
    }
  }

  private long nextSequence(Path parent) {
    Path lockFile = parent.resolve(LOCK);
    ReentrantLock jvmLock = JVM_LOCKS.computeIfAbsent(lockFile, k -> new ReentrantLock());
    jvmLock.lock();
    try (FileChannel channel =
        FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      FileLock lock = channel.lock();
      try {
        Path counter = parent.resolve(SEQUENCE);
        long next = 0L;
        if (Files.exists(counter)) {
          String text = new String(Files.readAllBytes(counter), StandardCharsets.UTF_8).trim();
          next = text.isEmpty() ? 0L : Long.parseLong(text);
        }
        write(counter, Long.toString(next + 1).getBytes(StandardCharsets.UTF_8));
        return next;
      } finally {
        lock.release();
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Could not allocate a sequence number under " + parent, e);
    } finally {
      jvmLock.unlock();
    }
  }

  Path resolve(String zkPath) {
    Path resolved = root;
    for (String segment : zkPath.split("/")) {
      if (segment.isEmpty() || segment.equals(".")) {
        continue;
      }
      if (segment.equals("..")) {
        throw new IllegalArgumentException("Path " + zkPath + " leaves the registry");
      }
      resolved = resolved.resolve(segment);
    }
    return resolved;
  }

  private String pathOf(Path node) {
    return "/" + root.relativize(node).toString().replace(java.io.File.separatorChar, '/');
  }

  private static List<String> children(Path dir) {
    if (!Files.isDirectory(dir)) {
      return List.of();
    }
    try (Stream<Path> entries = Files.list(dir)) {
      return entries
          .filter(Files::isDirectory)
          .map(p -> p.getFileName().toString())
          .filter(n -> !n.startsWith("."))
          .sorted()
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException("Could not list " + dir, e);
    }
  }

  private static void mkdirs(Path dir) {
    try {
      Files.createDirectories(dir);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not create " + dir, e);
    }
  }

  private static void write(Path file, byte[] data) {
    try {
      Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
      Files.write(tmp, data);
      Files.move(
          tmp,
          file,
          java.nio.file.StandardCopyOption.REPLACE_EXISTING,
          java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not write " + file, e);
    }
  }

  private static void deleteTree(Path node) {
    try (Stream<Path> tree = Files.walk(node)) {
      tree.sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException e) {
                  throw new UncheckedIOException("Could not delete " + p, e);
                }
              });
    } catch (IOException e) {
      throw new UncheckedIOException("Could not delete " + node, e);
    }
  }

  private enum Mode {
    PERSISTENT(false, false),
    PERSISTENT_SEQUENTIAL(false, true),
    EPHEMERAL(true, false),
    EPHEMERAL_SEQUENTIAL(true, true);

    final boolean ephemeral;
    final boolean sequential;

    Mode(boolean ephemeral, boolean sequential) {
      this.ephemeral = ephemeral;
      this.sequential = sequential;
    }

    static Mode of(String name) {
      return valueOf(name.toUpperCase(Locale.ROOT));
    }
  }

  /** The process a node belongs to: pid and start time, so a reused pid does not pass for it. */
  static final class Owner {
    final long pid;
    final long startedAtMillis;

    Owner(long pid, long startedAtMillis) {
      this.pid = pid;
      this.startedAtMillis = startedAtMillis;
    }

    static Owner self() {
      ProcessHandle me = ProcessHandle.current();
      return new Owner(me.pid(), startOf(me));
    }

    static Owner parse(String text) {
      String[] parts = text.trim().split("\\s+");
      return new Owner(Long.parseLong(parts[0]), parts.length > 1 ? Long.parseLong(parts[1]) : 0L);
    }

    boolean isAlive() {
      Optional<ProcessHandle> handle = ProcessHandle.of(pid).filter(ProcessHandle::isAlive);
      if (!handle.isPresent()) {
        return false;
      }
      long started = startOf(handle.get());
      return startedAtMillis == 0L || started == 0L || started == startedAtMillis;
    }

    private static long startOf(ProcessHandle p) {
      return p.info().startInstant().map(i -> i.toEpochMilli()).orElse(0L);
    }

    @Override
    public String toString() {
      return pid + " " + startedAtMillis;
    }
  }

  /** {@code host:port}, or the {@code hive.server2.thrift.*} form published configs use. */
  static final class HostPort {
    final String host;
    final int port;

    HostPort(String host, int port) {
      this.host = host;
      this.port = port;
    }

    static HostPort parse(String instance) {
      Map<String, String> pairs = attributesOf(instance);
      if (!pairs.isEmpty()) {
        return new HostPort(
            pairs.get("hive.server2.thrift.bind.host"),
            Integer.parseInt(pairs.get("hive.server2.thrift.port")));
      }
      int colon = instance.lastIndexOf(':');
      return new HostPort(
          instance.substring(0, colon), Integer.parseInt(instance.substring(colon + 1)));
    }
  }
}
