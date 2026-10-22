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

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Hive Metastore cluster behind the Services labeled {@code app.apache.org/discovery: HMS}.
 *
 * <p>The vocabulary is Hive's own, shared with the metastore's Kubernetes resolver so a cluster
 * reads the same to an HMS client and to a Kyuubi server; annotations under {@code
 * hive.apache.org/}:
 *
 * <pre>
 *   scheme    thrift (default) or http
 *   port      the thrift endpoint's port, a number or a port name; the Service port named
 *             "thrift" unless set, or its first port
 *   cluster   the logical cluster name; the Service name unless set
 *   default   "true" on the one cluster clients who name nothing get
 * </pre>
 *
 * A Service with an unknown {@code hive.apache.org/} key, an unknown scheme or a port it does not
 * have is skipped with a warning and costs only itself.
 *
 * <p>Unlike the Hive side, no EndpointSlices are read: the URI names the Service itself, {@code
 * scheme://<name>.<namespace>.svc:<port>}, and Kubernetes picks a Ready pod behind it, as it does
 * for a Trino cluster. The cluster is selected as on the Hive side: the only one there is, or the
 * one marked {@code default} among several; several and no unambiguous default selects none.
 */
final class HiveMetastores {

  static final String HMS_TYPE = "HMS";
  static final String PREFIX = "hive.apache.org/";
  static final String SCHEME_KEY = PREFIX + "scheme";
  static final String PORT_KEY = PREFIX + "port";
  static final String CLUSTER_KEY = PREFIX + "cluster";
  static final String DEFAULT_KEY = PREFIX + "default";
  static final String DEFAULT_PORT_NAME = "thrift";
  static final String METASTORE_URIS_KEY = "hive.metastore.uris";

  private static final Set<String> KNOWN_KEYS =
      Set.of(SCHEME_KEY, PORT_KEY, CLUSTER_KEY, DEFAULT_KEY);
  private static final Set<String> KNOWN_SCHEMES = Set.of("thrift", "http");

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastores.class);

  private HiveMetastores() {}

  /** Whether the Service declares a metastore cluster rather than an engine profile. */
  static boolean isHms(Service service) {
    Map<String, String> labels = service.getMetadata().getLabels();
    return labels != null
        && HMS_TYPE.equals(
            labels
                .getOrDefault(ServiceProfiles.DISCOVERY_LABEL, "")
                .trim()
                .toUpperCase(Locale.ROOT));
  }

  /**
   * The URI of the selected metastore cluster, or empty when no Service makes one or several
   * clusters leave the choice open.
   */
  static Optional<String> uriOf(List<Service> services) {
    Map<String, List<Service>> clusters = new LinkedHashMap<>();
    for (Service service : services) {
      if (resolve(service).isEmpty()) {
        continue;
      }
      String cluster = clusterOf(service);
      clusters.computeIfAbsent(cluster, k -> new ArrayList<>()).add(service);
    }
    if (clusters.isEmpty()) {
      return Optional.empty();
    }
    List<Service> selected;
    if (clusters.size() == 1) {
      selected = clusters.values().iterator().next();
    } else {
      List<String> defaults = new ArrayList<>();
      for (Map.Entry<String, List<Service>> e : clusters.entrySet()) {
        if (e.getValue().stream()
            .anyMatch(
                s -> Boolean.parseBoolean(annotationsOf(s).getOrDefault(DEFAULT_KEY, "false")))) {
          defaults.add(e.getKey());
        }
      }
      if (defaults.size() != 1) {
        LOG.warn(
            "No metastore cluster selected out of {}: {}; mark exactly one with '{}: \"true\"'",
            clusters.size(),
            clusters.keySet(),
            DEFAULT_KEY);
        return Optional.empty();
      }
      selected = clusters.get(defaults.get(0));
    }
    List<Service> sorted = new ArrayList<>(selected);
    sorted.sort(
        Comparator.comparing((Service s) -> s.getMetadata().getNamespace())
            .thenComparing(s -> s.getMetadata().getName()));
    Service first = sorted.get(0);
    if (sorted.size() > 1) {
      LOG.warn(
          "Metastore cluster '{}' is {} Services; {} is used",
          clusterOf(first),
          sorted.size(),
          first.getMetadata().getNamespace() + "/" + first.getMetadata().getName());
    }
    Resolved resolved = resolve(first).get();
    return Optional.of(
        resolved.scheme
            + "://"
            + first.getMetadata().getName()
            + "."
            + first.getMetadata().getNamespace()
            + ".svc:"
            + resolved.port);
  }

  private static String clusterOf(Service service) {
    String name = service.getMetadata().getName();
    return Optional.ofNullable(annotationsOf(service).get(CLUSTER_KEY))
        .map(String::trim)
        .filter(n -> !n.isEmpty())
        .orElse(name);
  }

  /**
   * The scheme and port the Service serves its metastore on, or empty when the Service is skipped:
   * an unknown key, an unknown scheme, or a port it does not have.
   */
  private static Optional<Resolved> resolve(Service service) {
    String namespace = service.getMetadata().getNamespace();
    String name = service.getMetadata().getName();
    Map<String, String> annotations = annotationsOf(service);
    for (String key : annotations.keySet()) {
      if (key.startsWith(PREFIX) && !KNOWN_KEYS.contains(key)) {
        LOG.warn(
            "Skipping Service {}/{}: '{}' is not a metastore discovery key; expected one of {}",
            namespace,
            name,
            key,
            KNOWN_KEYS);
        return Optional.empty();
      }
    }
    String scheme = annotations.getOrDefault(SCHEME_KEY, "thrift").toLowerCase(Locale.ROOT).trim();
    if (!KNOWN_SCHEMES.contains(scheme)) {
      LOG.warn(
          "Skipping Service {}/{}: '{}' is not a known scheme; expected thrift or http",
          namespace,
          name,
          scheme);
      return Optional.empty();
    }
    List<ServicePort> ports =
        service.getSpec() == null || service.getSpec().getPorts() == null
            ? List.of()
            : service.getSpec().getPorts();
    String requested = annotations.getOrDefault(PORT_KEY, "").trim();
    if (!requested.isEmpty()) {
      if (requested.chars().allMatch(Character::isDigit)) {
        return Optional.of(new Resolved(scheme, Integer.parseInt(requested)));
      }
      return ports.stream()
          .filter(p -> requested.equals(p.getName()))
          .findFirst()
          .map(p -> new Resolved(scheme, p.getPort()))
          .or(
              () -> {
                LOG.warn(
                    "Skipping Service {}/{}: the Service has no port named {}",
                    namespace,
                    name,
                    requested);
                return Optional.empty();
              });
    }
    if (ports.isEmpty()) {
      LOG.warn("Skipping Service {}/{}: the Service has no ports", namespace, name);
      return Optional.empty();
    }
    ServicePort port =
        ports.stream()
            .filter(p -> DEFAULT_PORT_NAME.equals(p.getName()))
            .findFirst()
            .orElse(ports.get(0));
    return Optional.of(new Resolved(scheme, port.getPort()));
  }

  private static Map<String, String> annotationsOf(Service service) {
    Map<String, String> annotations = service.getMetadata().getAnnotations();
    return annotations != null ? annotations : Map.of();
  }

  private static final class Resolved {
    private final String scheme;
    private final int port;

    private Resolved(String scheme, int port) {
      this.scheme = scheme;
      this.port = port;
    }
  }
}
