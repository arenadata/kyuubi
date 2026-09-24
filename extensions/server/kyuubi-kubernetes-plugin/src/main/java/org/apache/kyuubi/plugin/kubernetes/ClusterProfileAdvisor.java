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

import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.plugin.SessionConfAdvisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Applies to a session the engine profile its cluster's Service declares.
 *
 * <p>The server resolves declared profiles itself, from {@code kyuubi.engine.profile.<name>.*} in
 * its configuration; this adds the ones declared on Services, under the same name and the same
 * session key. A session that asks for a profile by name gets the Service's if one is called that;
 * a session that asks for none gets the one naming its user, or the default. What comes back is a
 * conf overlay, which the server puts on top of the client's own settings - so a client that sets a
 * catalog keeps it.
 *
 * <p>A name that is neither a Service's profile nor a declared one fails the session open, here,
 * with the names that exist. The server's own check for that has to be set to log rather than fail
 * ({@code kyuubi.engine.profiles.unknown.strategy=LOG}), because it runs before this does and knows
 * nothing of the Services.
 *
 * <p>No lifecycle is offered to a session conf advisor - the server only constructs it - so the
 * catalog is started on first use, from the same configuration file the server read, and stopped
 * from a shutdown hook.
 */
public class ClusterProfileAdvisor implements SessionConfAdvisor {

  public static final String NAMESPACES_KEY = "kyuubi.plugin.kubernetes.namespaces";
  public static final String LABEL_SELECTOR_KEY = "kyuubi.plugin.kubernetes.labelSelector";
  public static final String ANNOTATION_PREFIX_KEY = "kyuubi.plugin.kubernetes.annotationPrefix";
  public static final String DEFAULT_LABEL_SELECTOR = "kyuubi/enabled=true";

  static final String PROFILE_KEY = "kyuubi.engine.profile";
  static final String DECLARED_PROFILE_PREFIX = "kyuubi.engine.profile.";
  private static final String USER_PLACEHOLDER = "{user}";

  private static final Logger LOG = LoggerFactory.getLogger(ClusterProfileAdvisor.class);

  private volatile ClusterCatalog catalog;
  private volatile Set<String> declaredProfiles;

  public ClusterProfileAdvisor() {}

  /** For tests, and for anyone with a catalog of their own. */
  ClusterProfileAdvisor(ClusterCatalog catalog, Set<String> declaredProfiles) {
    this.catalog = catalog;
    this.declaredProfiles = Set.copyOf(declaredProfiles);
  }

  @Override
  public Map<String, String> getConfOverlay(String user, Map<String, String> sessionConf) {
    ensureStarted();
    String requested =
        Optional.ofNullable(sessionConf.get(PROFILE_KEY)).map(String::trim).orElse("");
    Optional<ClusterProfile> chosen;
    if (!requested.isEmpty()) {
      chosen = catalog.byName(requested);
      if (chosen.isEmpty()) {
        if (declaredProfiles.contains(requested)) {
          return Map.of();
        }
        throw Unchecked.sqlException(
            "Engine profile '"
                + requested
                + "' is not defined. Profiles from Services: "
                + names(catalog.profiles())
                + ", declared profiles: "
                + declaredProfiles.stream().sorted().collect(Collectors.toList())
                + ".",
            null);
      }
      if (!chosen.get().allows(user)) {
        throw Unchecked.sqlException(
            "Engine profile '" + requested + "' is not allowed for user " + user + ".", null);
      }
    } else {
      chosen = catalog.forUser(user);
      if (chosen.isEmpty()) {
        return Map.of();
      }
    }
    ClusterProfile profile = chosen.get();
    Map<String, String> overlay = new LinkedHashMap<>();
    profile.conf().forEach((k, v) -> overlay.put(k, v.replace(USER_PLACEHOLDER, user)));
    LOG.info(
        "Applying engine profile '{}' from Service {} to the session of {}",
        profile.name(),
        profile.source(),
        user);
    return overlay;
  }

  private static List<String> names(List<ClusterProfile> profiles) {
    return profiles.stream().map(ClusterProfile::name).sorted().collect(Collectors.toList());
  }

  private void ensureStarted() {
    if (catalog != null) {
      return;
    }
    synchronized (this) {
      if (catalog != null) {
        return;
      }
      KyuubiConf conf = new KyuubiConf(true).loadFileDefaults();
      declaredProfiles = declaredProfilesIn(conf);
      KubernetesClusterCatalog started =
          new KubernetesClusterCatalog(new KubernetesClientBuilder().build(), settingsFrom(conf));
      started.start();
      Runtime.getRuntime().addShutdownHook(new Thread(started::close, "cluster-catalog-stop"));
      LOG.info(
          "Watching Services for engine profiles in {}; {} known at start",
          settingsFrom(conf).namespaces().isEmpty()
              ? "all namespaces"
              : settingsFrom(conf).namespaces(),
          started.profiles().size());
      catalog = started;
    }
  }

  static KubernetesClusterCatalog.Settings settingsFrom(KyuubiConf conf) {
    return new KubernetesClusterCatalog.Settings(
        csv(option(conf, NAMESPACES_KEY).orElse("")),
        option(conf, LABEL_SELECTOR_KEY).orElse(DEFAULT_LABEL_SELECTOR),
        option(conf, ANNOTATION_PREFIX_KEY).orElse(ServiceProfiles.DEFAULT_PREFIX));
  }

  /** The names under {@code kyuubi.engine.profile.<name>.*}: what the server resolves itself. */
  static Set<String> declaredProfilesIn(KyuubiConf conf) {
    return JavaConverters.mapAsJavaMap(conf.getAll()).keySet().stream()
        .filter(k -> k.startsWith(DECLARED_PROFILE_PREFIX))
        .map(k -> k.substring(DECLARED_PROFILE_PREFIX.length()).split("\\.", 2)[0])
        .filter(n -> !n.isEmpty())
        .collect(Collectors.toUnmodifiableSet());
  }

  private static Optional<String> option(KyuubiConf conf, String key) {
    scala.Option<String> value = conf.getOption(key);
    return value.isDefined()
        ? Optional.of(value.get()).filter(v -> !v.isBlank())
        : Optional.empty();
  }

  private static List<String> csv(String value) {
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(v -> !v.isEmpty())
        .collect(Collectors.toList());
  }
}
