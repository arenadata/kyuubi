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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads an engine profile out of the annotations of the Service in front of a cluster.
 *
 * <p>The vocabulary is the one {@code kyuubi.engine.profile.<name>.*} already has, so declaring a
 * cluster on its Service and declaring it in {@code kyuubi-defaults.conf} say the same thing:
 *
 * <pre>
 *   type                 the engine type, e.g. TRINO or JDBC
 *   env.VAR              an environment variable for the engine
 *   session.key          a Kyuubi session config, kyuubi.session.key
 *   conf.key             any config key, as is
 * </pre>
 *
 * plus what only a Service needs to say:
 *
 * <pre>
 *   profile              the profile name; the Service name unless set
 *   users                who may use it, comma separated; anyone if unset
 *   default              "true" on the one profile users who ask for nothing get
 *   scheme, port         how to reach the cluster through this Service; http and its first port
 * </pre>
 *
 * <p>Every value may say {@code {host}}, {@code {port}} and {@code {url}} for the Service's own
 * address, and {@code {user}} for the session user - the latter is filled in per session. A TRINO
 * profile that names no connection url gets the Service's.
 *
 * <p>Keys carry one prefix, {@code kyuubi/} by default, and a Service with a key outside the
 * vocabulary is refused as a whole: a typo must show as a missing cluster, not as a cluster that
 * quietly ignores half its declaration.
 */
public final class ServiceProfiles {

  public static final String DEFAULT_PREFIX = "kyuubi/";
  static final String ENGINE_TYPE_KEY = "kyuubi.engine.type";
  static final String ENGINE_ENV_PREFIX = "kyuubi.engineEnv.";
  static final String SESSION_PREFIX = "kyuubi.session.";
  static final String SUBDOMAIN_KEY = "kyuubi.engine.share.level.subdomain";
  static final String TRINO_URL_KEY = "kyuubi.engine.trino.connection.url";

  private static final int DEFAULT_PORT = 8080;
  private static final Set<String> OWN_KEYS =
      Set.of("profile", "users", "default", "scheme", "port");

  private ServiceProfiles() {}

  /** @throws IllegalArgumentException when the annotations do not make a profile */
  public static ClusterProfile toProfile(Service service, String prefix) {
    String namespace = service.getMetadata().getNamespace();
    String serviceName = service.getMetadata().getName();
    Map<String, String> ann = new LinkedHashMap<>();
    Optional.ofNullable(service.getMetadata().getAnnotations())
        .orElseGet(Map::of)
        .forEach(
            (key, value) -> {
              if (key.startsWith(prefix)) {
                ann.put(key.substring(prefix.length()), value);
              }
            });

    String scheme = ann.getOrDefault("scheme", "http");
    int port = resolvePort(service, Optional.ofNullable(ann.get("port")));
    String host = serviceName + "." + namespace + ".svc";
    String url = scheme + "://" + host + ":" + port;

    Map<String, String> conf = new LinkedHashMap<>();
    for (Map.Entry<String, String> e : ann.entrySet()) {
      String key = e.getKey();
      String value =
          e.getValue()
              .replace("{host}", host)
              .replace("{port}", Integer.toString(port))
              .replace("{url}", url);
      if (OWN_KEYS.contains(key)) {
        continue;
      }
      String[] parts = key.split("\\.", 2);
      switch (parts[0]) {
        case "type":
          if (parts.length != 1) {
            throw new IllegalArgumentException("'" + prefix + key + "': 'type' takes no sub-key");
          }
          conf.put(ENGINE_TYPE_KEY, value.toUpperCase(Locale.ROOT));
          break;
        case "env":
          conf.put(ENGINE_ENV_PREFIX + subKey(prefix, key, parts), value);
          break;
        case "session":
          conf.put(SESSION_PREFIX + subKey(prefix, key, parts), value);
          break;
        case "conf":
          conf.put(subKey(prefix, key, parts), value);
          break;
        default:
          throw new IllegalArgumentException(
              "'"
                  + prefix
                  + key
                  + "' is not a profile key; expected type, env.<VAR>, session.<key>,"
                  + " conf.<key>, profile, users, default, scheme or port");
      }
    }
    if (!conf.containsKey(ENGINE_TYPE_KEY)) {
      throw new IllegalArgumentException("'" + prefix + "type' is missing");
    }
    if ("TRINO".equals(conf.get(ENGINE_TYPE_KEY))) {
      conf.putIfAbsent(TRINO_URL_KEY, url);
    }

    String name =
        Optional.ofNullable(ann.get("profile"))
            .map(String::trim)
            .filter(n -> !n.isEmpty())
            .orElse(serviceName);
    // Engines are keyed by share level, type, user and subdomain, not by where they connect:
    // without this, two profiles at the same share level would share one engine, pointed at
    // whichever cluster it was launched for.
    conf.putIfAbsent(SUBDOMAIN_KEY, subdomainOf(name));

    return new ClusterProfile(
        name,
        namespace + "/" + serviceName,
        csv(ann.get("users")),
        Boolean.parseBoolean(ann.getOrDefault("default", "false")),
        conf);
  }

  /** A profile name as a path segment: a subdomain ends up in a discovery path. */
  static String subdomainOf(String name) {
    StringBuilder out = new StringBuilder();
    for (char c : name.toLowerCase(Locale.ROOT).toCharArray()) {
      out.append(Character.isLetterOrDigit(c) ? c : '-');
    }
    return out.toString();
  }

  private static String subKey(String prefix, String key, String[] parts) {
    if (parts.length != 2 || parts[1].isEmpty()) {
      throw new IllegalArgumentException("'" + prefix + key + "' needs a sub-key");
    }
    return parts[1];
  }

  private static int resolvePort(Service service, Optional<String> requested) {
    List<ServicePort> ports =
        service.getSpec() == null || service.getSpec().getPorts() == null
            ? List.of()
            : service.getSpec().getPorts();
    if (requested.isEmpty()) {
      return ports.isEmpty() ? DEFAULT_PORT : ports.get(0).getPort();
    }
    String wanted = requested.get().trim();
    if (!wanted.isEmpty() && wanted.chars().allMatch(Character::isDigit)) {
      return Integer.parseInt(wanted);
    }
    return ports.stream()
        .filter(p -> wanted.equals(p.getName()))
        .map(ServicePort::getPort)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("the Service has no port named " + wanted));
  }

  private static Set<String> csv(String value) {
    if (value == null) {
      return Set.of();
    }
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(v -> !v.isEmpty())
        .collect(Collectors.toUnmodifiableSet());
  }
}
