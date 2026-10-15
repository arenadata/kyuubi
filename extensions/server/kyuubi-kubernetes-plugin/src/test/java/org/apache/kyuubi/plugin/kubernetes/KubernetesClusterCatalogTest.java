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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.Rule;
import org.junit.Test;

public class KubernetesClusterCatalogTest {

  @Rule public KubernetesServer server = new KubernetesServer(false, true);

  private static Service service(String namespace, String name, Map<String, String> annotations) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withNamespace(namespace)
        .withName(name)
        .withLabels(Map.of("kyuubi/enabled", "true"))
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
    server
        .getClient()
        .services()
        .inNamespace("trino")
        .resource(service("trino", "analytics", Map.of("kyuubi/type", "TRINO")))
        .create();
    server
        .getClient()
        .services()
        .inNamespace("elsewhere")
        .resource(service("elsewhere", "hidden", Map.of("kyuubi/type", "TRINO")))
        .create();

    try (KubernetesClusterCatalog catalog =
        new KubernetesClusterCatalog(
            server.getClient(),
            new KubernetesClusterCatalog.Settings(
                List.of("trino"), "kyuubi/enabled=true", "kyuubi/"))) {
      catalog.start();
      assertEquals(
          List.of("analytics"), catalog.profiles().stream().map(ClusterProfile::name).toList());
      assertTrue(catalog.isWatching());

      // Re-declared: the change lands without anything being restarted.
      server
          .getClient()
          .services()
          .inNamespace("trino")
          .resource(
              service(
                  "trino", "analytics", Map.of("kyuubi/type", "TRINO", "kyuubi/users", "alice")))
          .update();
      await(
          "the new annotation was never seen",
          () -> catalog.byName("analytics").map(p -> p.users().contains("alice")).orElse(false));

      // A Service that stops being a profile costs only itself.
      server
          .getClient()
          .services()
          .inNamespace("trino")
          .resource(service("trino", "broken", Map.of("kyuubi/nonsense", "x")))
          .create();
      server
          .getClient()
          .services()
          .inNamespace("trino")
          .resource(service("trino", "etl", Map.of("kyuubi/type", "JDBC")))
          .create();
      await("the good Service was never seen", () -> catalog.byName("etl").isPresent());
      assertEquals(2, catalog.profiles().size());

      server.getClient().services().inNamespace("trino").withName("analytics").delete();
      await("the deleted Service is still listed", () -> catalog.byName("analytics").isEmpty());
    }
  }
}
