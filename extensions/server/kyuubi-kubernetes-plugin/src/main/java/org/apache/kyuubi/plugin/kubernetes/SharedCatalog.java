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
import java.util.function.Supplier;
import org.apache.kyuubi.config.KyuubiConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one {@link KubernetesClusterCatalog} of a server.
 *
 * <p>The advisor and the discovery client are separate plugins the server constructs on its own,
 * and both need the same answer: which profiles the Services declare, and where their engines run.
 * One informer per namespace serves both, started by whichever asks first.
 */
final class SharedCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(SharedCatalog.class);
  private static volatile ClusterCatalog catalog;

  private SharedCatalog() {}

  static ClusterCatalog get(KyuubiConf conf) {
    return get(conf, () -> new KubernetesClientBuilder().build());
  }

  static synchronized ClusterCatalog get(
      KyuubiConf conf, Supplier<io.fabric8.kubernetes.client.KubernetesClient> client) {
    if (catalog == null) {
      KubernetesClusterCatalog.Settings settings = ClusterProfileAdvisor.settingsFrom(conf);
      KubernetesClusterCatalog started = new KubernetesClusterCatalog(client.get(), settings);
      started.start();
      Runtime.getRuntime().addShutdownHook(new Thread(started::close, "cluster-catalog-stop"));
      LOG.info(
          "Watching Services for engine profiles in {}; {} known at start",
          settings.namespaces().isEmpty() ? "all namespaces" : settings.namespaces(),
          started.profiles().size());
      catalog = started;
    }
    return catalog;
  }

  /** For tests: the catalog both plugins will use. */
  static synchronized void set(ClusterCatalog shared) {
    catalog = shared;
  }
}
