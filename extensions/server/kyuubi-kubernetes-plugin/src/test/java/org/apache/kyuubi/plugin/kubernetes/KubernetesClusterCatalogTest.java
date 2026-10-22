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

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.Rule;
import org.junit.Test;

public class KubernetesClusterCatalogTest {

  private static final String SELECTOR = ClusterProfileAdvisor.DEFAULT_LABEL_SELECTOR;
  private static final String PREFIX = ServiceProfiles.DEFAULT_PREFIX;
  private static final String LABEL = ServiceProfiles.DISCOVERY_LABEL;

  @Rule public KubernetesServer server = new KubernetesServer(false, true);

  private static Service service(
      String namespace, String name, String type, Map<String, String> annotations) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withNamespace(namespace)
        .withName(name)
        .withLabels(Map.of(LABEL, type))
        .withAnnotations(annotations)
        .endMetadata()
        .withNewSpec()
        .addNewPort()
        .withName("http")
        .withPort(8080)
        .endPort()
        .endSpec()
        .build();
  }

  private static Service hms(
      String namespace, String name, Map<String, String> annotations, ServicePort... ports) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withNamespace(namespace)
        .withName(name)
        .withLabels(Map.of(LABEL, HiveMetastores.HMS_TYPE))
        .withAnnotations(annotations)
        .endMetadata()
        .withNewSpec()
        .withPorts(List.of(ports))
        .endSpec()
        .build();
  }

  private static ServicePort port(String name, int number) {
    ServicePort p = new ServicePort();
    p.setName(name);
    p.setPort(number);
    return p;
  }

  private void create(String namespace, Service service) {
    server.getClient().services().inNamespace(namespace).resource(service).createOrReplace();
  }

  private KubernetesClusterCatalog startCatalog(List<String> namespaces) {
    KubernetesClusterCatalog catalog =
        new KubernetesClusterCatalog(
            server.getClient(),
            new KubernetesClusterCatalog.Settings(namespaces, SELECTOR, PREFIX));
    catalog.start();
    return catalog;
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    fail(what);
  }

  @Test
  public void profilesFollowTheServicesInTheListedNamespaces() throws Exception {
    create("trino", service("trino", "analytics", "TRINO", Map.of()));
    create("elsewhere", service("elsewhere", "hidden", "TRINO", Map.of()));

    try (KubernetesClusterCatalog catalog = startCatalog(List.of("trino"))) {
      assertEquals(
          List.of("analytics"), catalog.profiles().stream().map(ClusterProfile::name).toList());
      assertTrue(catalog.isWatching());

      // Re-declared: the change lands without anything being restarted.
      create("trino", service("trino", "analytics", "TRINO", Map.of(PREFIX + "users", "alice")));
      await(
          "the new annotation was never seen",
          () -> catalog.byName("analytics").map(p -> p.users().contains("alice")).orElse(false));

      // A Service that stops being a profile costs only itself.
      create("trino", service("trino", "broken", "TRINO", Map.of(PREFIX + "nonsense", "x")));
      create("trino", service("trino", "etl", "JDBC", Map.of()));
      await("the good Service was never seen", () -> catalog.byName("etl").isPresent());
      assertEquals(2, catalog.profiles().size());

      server.getClient().services().inNamespace("trino").withName("analytics").delete();
      await("the deleted Service is still listed", () -> catalog.byName("analytics").isEmpty());
    }
  }

  @Test
  public void aMetastoreIsNoProfileAndItsUriIsInjectedIntoTheProfilesThatLackIt() throws Exception {
    create("trino", service("trino", "analytics", "TRINO", Map.of(PREFIX + "users", "alice")));
    create("trino", service("trino", "etl", "JDBC", Map.of()));
    create("hive", hms("hive", "metastore", Map.of(), port("web", 8080), port("thrift", 9083)));

    try (KubernetesClusterCatalog catalog = startCatalog(List.of("trino", "hive"))) {
      await(
          "the metastore uri was never injected",
          () ->
              catalog
                  .byName("etl")
                  .map(p -> p.conf().containsKey("hive.metastore.uris"))
                  .orElse(false));
      // The HMS Service is no profile: nothing lists it, nothing answers an engine space for it.
      assertTrue(catalog.byName("metastore").isEmpty());
      assertEquals(2, catalog.profiles().size());
      // The Service port named 'thrift' wins over the first port.
      assertEquals(
          "thrift://metastore.hive.svc:9083",
          catalog.byName("etl").get().conf().get("hive.metastore.uris"));
      assertEquals(
          "thrift://metastore.hive.svc:9083",
          catalog.byName("analytics").get().conf().getOrDefault("hive.metastore.uris", ""));
    }
  }

  @Test
  public void aProfileThatNamesItsOwnMetastoreKeepsIt() throws Exception {
    create(
        "trino",
        service(
            "trino",
            "analytics",
            "TRINO",
            Map.of(PREFIX + "conf.hive.metastore.uris", "thrift://its-own:1")));
    create("hive", hms("hive", "metastore", Map.of(), port("thrift", 9083)));

    try (KubernetesClusterCatalog catalog = startCatalog(List.of("trino", "hive"))) {
      await("the profile was never seen", () -> catalog.byName("analytics").isPresent());
      assertEquals(
          "thrift://its-own:1",
          catalog.byName("analytics").get().conf().get("hive.metastore.uris"));
    }
  }

  @Test
  public void theDefaultMarkedClusterIsSelectedAmongSeveral() throws Exception {
    create("trino", service("trino", "analytics", "TRINO", Map.of()));
    create(
        "hive", hms("hive", "main", Map.of(HiveMetastores.CLUSTER_KEY, "a"), port("thrift", 9083)));
    create(
        "hive",
        hms(
            "hive",
            "backup",
            Map.of(HiveMetastores.CLUSTER_KEY, "b", HiveMetastores.DEFAULT_KEY, "true"),
            port("thrift", 9084)));

    try (KubernetesClusterCatalog catalog = startCatalog(List.of("trino", "hive"))) {
      await(
          "the default cluster was never selected",
          () ->
              catalog
                  .byName("analytics")
                  .map(p -> p.conf().containsKey("hive.metastore.uris"))
                  .orElse(false));
      assertEquals(
          "thrift://backup.hive.svc:9084",
          catalog.byName("analytics").get().conf().get("hive.metastore.uris"));
    }
  }

  @Test
  public void severalClustersWithoutADefaultSelectNone() throws Exception {
    create("trino", service("trino", "analytics", "TRINO", Map.of()));
    create(
        "hive", hms("hive", "main", Map.of(HiveMetastores.CLUSTER_KEY, "a"), port("thrift", 9083)));
    create(
        "hive",
        hms("hive", "backup", Map.of(HiveMetastores.CLUSTER_KEY, "b"), port("thrift", 9084)));

    try (KubernetesClusterCatalog catalog = startCatalog(List.of("trino", "hive"))) {
      await("the profile was never seen", () -> catalog.byName("analytics").isPresent());
      // Rebuilds settle once both Services are listed; no uri may ever appear.
      for (int i = 0; i < 20; i++) {
        assertFalse(catalog.byName("analytics").get().conf().containsKey("hive.metastore.uris"));
        Thread.sleep(50);
      }
    }
  }

  @Test
  public void schemeAndPortAreTheServicesToSay() throws Exception {
    create("trino", service("trino", "analytics", "TRINO", Map.of()));
    create(
        "hive",
        hms(
            "hive",
            "metastore",
            Map.of(HiveMetastores.SCHEME_KEY, "http", HiveMetastores.PORT_KEY, "http-api"),
            port("thrift", 9083),
            port("http-api", 10001)));

    try (KubernetesClusterCatalog catalog = startCatalog(List.of("trino", "hive"))) {
      await(
          "the metastore uri was never injected",
          () ->
              catalog
                  .byName("analytics")
                  .map(p -> p.conf().containsKey("hive.metastore.uris"))
                  .orElse(false));
      assertEquals(
          "http://metastore.hive.svc:10001",
          catalog.byName("analytics").get().conf().get("hive.metastore.uris"));
    }
  }

  @Test
  public void aNumberedPortAndTheFirstPortFallback() throws Exception {
    create("trino", service("trino", "analytics", "TRINO", Map.of()));
    create(
        "hive",
        hms("hive", "numbered", Map.of(HiveMetastores.PORT_KEY, " 9999 "), port("thrift", 9083)));

    try (KubernetesClusterCatalog catalog = startCatalog(List.of("trino", "hive"))) {
      await(
          "the numbered port was never used",
          () ->
              catalog
                  .byName("analytics")
                  .map(p -> p.conf().containsKey("hive.metastore.uris"))
                  .orElse(false));
      assertEquals(
          "thrift://numbered.hive.svc:9999",
          catalog.byName("analytics").get().conf().get("hive.metastore.uris"));

      // No port annotation and no port named 'thrift': the Service's first port.
      create("hive", hms("hive", "first", Map.of(), port("web", 8080)));
      // 'first' replaces 'numbered' as the only cluster once 'numbered' is gone.
      server.getClient().services().inNamespace("hive").withName("numbered").delete();
      await(
          "the first port was never used",
          () ->
              catalog
                  .byName("analytics")
                  .map(
                      p ->
                          "thrift://first.hive.svc:8080"
                              .equals(p.conf().get("hive.metastore.uris")))
                  .orElse(false));
    }
  }

  @Test
  public void aPortTheServiceDoesNotHaveCostsOnlyItself() throws Exception {
    create("trino", service("trino", "analytics", "TRINO", Map.of()));
    create(
        "hive",
        hms("hive", "broken", Map.of(HiveMetastores.PORT_KEY, "thirft"), port("thrift", 9083)));

    try (KubernetesClusterCatalog catalog = startCatalog(List.of("trino", "hive"))) {
      await("the profile was never seen", () -> catalog.byName("analytics").isPresent());
      for (int i = 0; i < 20; i++) {
        assertFalse(catalog.byName("analytics").get().conf().containsKey("hive.metastore.uris"));
        Thread.sleep(50);
      }

      // Fixed, the same Service serves the cluster.
      create(
          "hive",
          hms("hive", "broken", Map.of(HiveMetastores.PORT_KEY, "thrift"), port("thrift", 9083)));
      await(
          "the fixed Service was never seen",
          () ->
              catalog
                  .byName("analytics")
                  .map(
                      p ->
                          "thrift://broken.hive.svc:9083"
                              .equals(p.conf().get("hive.metastore.uris")))
                  .orElse(false));
    }
  }

  @Test
  public void anUnknownMetastoreKeyCostsOnlyItsService() throws Exception {
    create("trino", service("trino", "analytics", "TRINO", Map.of()));
    create(
        "hive",
        hms("hive", "typo", Map.of("hive.apache.org/scheeme", "http"), port("thrift", 9083)));

    try (KubernetesClusterCatalog catalog = startCatalog(List.of("trino", "hive"))) {
      await("the profile was never seen", () -> catalog.byName("analytics").isPresent());
      for (int i = 0; i < 20; i++) {
        assertFalse(catalog.byName("analytics").get().conf().containsKey("hive.metastore.uris"));
        Thread.sleep(50);
      }
    }
  }
}
