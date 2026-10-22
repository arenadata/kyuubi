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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class ServiceProfilesTest {

  private static final String PREFIX = ServiceProfiles.DEFAULT_PREFIX;

  private static Service service(String type, Map<String, String> annotations) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withNamespace("trino")
        .withName("analytics")
        .withLabels(Map.of(ServiceProfiles.DISCOVERY_LABEL, type))
        .withAnnotations(annotations)
        .endMetadata()
        .withNewSpec()
        .addNewPort()
        .withName("http")
        .withPort(8080)
        .endPort()
        .addNewPort()
        .withName("https")
        .withPort(8443)
        .endPort()
        .endSpec()
        .build();
  }

  private static ClusterProfile profile(String type, Map<String, String> annotations) {
    return ServiceProfiles.toProfile(service(type, annotations), PREFIX);
  }

  @Test
  public void aTrinoServiceIsAProfileWithItsOwnAddress() {
    ClusterProfile p = profile("trino", Map.of());
    assertEquals("analytics", p.name());
    assertEquals("trino/analytics", p.source());
    assertEquals("TRINO", p.conf().get("kyuubi.engine.type"));
    assertEquals(
        "http://analytics.trino.svc:8080",
        p.conf().get("kyuubi.session.engine.trino.connection.url"));
    assertEquals("analytics", p.conf().get("kyuubi.engine.share.level.subdomain"));
    assertTrue(p.users().isEmpty());
    assertFalse(p.isDefault());
    assertTrue(p.allows("anyone"));
  }

  @Test
  public void theTypeIsTheLabelAndTheLabelAlone() {
    Service unlabeled =
        new ServiceBuilder(service("TRINO", Map.of()))
            .editMetadata()
            .withLabels(Map.of())
            .endMetadata()
            .build();
    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class, () -> ServiceProfiles.toProfile(unlabeled, PREFIX));
    assertTrue(
        missing.getMessage(), missing.getMessage().contains(ServiceProfiles.DISCOVERY_LABEL));

    // 'type' is no longer a profile key: as any unknown key, it refuses the Service.
    assertThrows(
        IllegalArgumentException.class, () -> profile("TRINO", Map.of(PREFIX + "type", "TRINO")));
  }

  @Test
  public void theBucketsMapAsDeclaredProfilesDo() {
    ClusterProfile p =
        profile(
            "TRINO",
            Map.of(
                PREFIX + "env.TRINO_HOME", "/opt/trino",
                PREFIX + "session.engine.trino.connection.catalog", "hive",
                PREFIX + "conf.kyuubi.engine.trino.memory", "2g",
                PREFIX + "conf.kyuubi.engine.share.level.subdomain", "custom"));
    assertEquals("/opt/trino", p.conf().get("kyuubi.engineEnv.TRINO_HOME"));
    assertEquals("hive", p.conf().get("kyuubi.session.engine.trino.connection.catalog"));
    assertEquals("2g", p.conf().get("kyuubi.engine.trino.memory"));
    assertEquals(
        "an explicit subdomain is kept",
        "custom",
        p.conf().get("kyuubi.engine.share.level.subdomain"));
  }

  @Test
  public void placeholdersNameTheServiceAndTheUser() {
    ClusterProfile p =
        profile(
            "JDBC",
            Map.of(
                PREFIX + "port", "https",
                PREFIX + "scheme", "https",
                PREFIX + "conf.kyuubi.engine.jdbc.type", "impala",
                PREFIX + "conf.kyuubi.engine.jdbc.connection.url",
                    "jdbc:impala://{host}:{port}/;hive.server2.proxy.user={user}",
                PREFIX + "conf.some.url", "{url}"));
    assertEquals(
        "jdbc:impala://analytics.trino.svc:8443/;hive.server2.proxy.user={user}",
        p.conf().get("kyuubi.engine.jdbc.connection.url"));
    assertEquals("https://analytics.trino.svc:8443", p.conf().get("some.url"));
    assertFalse(
        "a JDBC profile gets no Trino url",
        p.conf().containsKey("kyuubi.session.engine.trino.connection.url"));
  }

  @Test
  public void nameUsersAndDefaultAreTheServicesToSay() {
    ClusterProfile p =
        profile(
            "TRINO",
            Map.of(
                PREFIX + "profile", "Adhoc Trino",
                PREFIX + "users", "alice, bob,",
                PREFIX + "default", "true"));
    assertEquals("Adhoc Trino", p.name());
    assertEquals("adhoc-trino", p.conf().get("kyuubi.engine.share.level.subdomain"));
    assertEquals(Set.of("alice", "bob"), p.users());
    assertTrue(p.isDefault());
    assertFalse(p.allows("carol"));
  }

  @Test
  public void aServiceThatIsNotAProfileIsRefusedWhole() {
    assertThrows(
        IllegalArgumentException.class, () -> profile("TRINO", Map.of(PREFIX + "typo.key", "x")));
    assertThrows(
        IllegalArgumentException.class, () -> profile("TRINO", Map.of(PREFIX + "port", "grpc")));
    assertThrows(
        IllegalArgumentException.class, () -> profile("TRINO", Map.of(PREFIX + "env", "x")));
  }

  @Test
  public void otherPrefixesAreSomebodyElses() {
    ClusterProfile p =
        ServiceProfiles.toProfile(
            service("TRINO", Map.of("hive.apache.org/scheme", "thrift")), PREFIX);
    assertEquals(3, p.conf().size());
  }

  @Test
  public void aPortNamedKyuubiIsWhereTheProfilesEngineRuns() {
    Service withEngine =
        new ServiceBuilder(service("SPARK_SQL", Map.of()))
            .editSpec()
            .addNewPort()
            .withName("kyuubi")
            .withPort(10009)
            .endPort()
            .endSpec()
            .build();
    ClusterProfile p = ServiceProfiles.toProfile(withEngine, PREFIX);
    assertEquals("analytics.trino.svc", p.engine().get().host());
    assertEquals(10009, p.engine().get().port());
    assertFalse(profile("TRINO", Map.of()).engine().isPresent());

    // The Service of a Trino engine is not Trino: no connection url is made up from it.
    Service trinoEngine =
        new ServiceBuilder(service("TRINO", Map.of()))
            .editSpec()
            .addNewPort()
            .withName("kyuubi")
            .withPort(10009)
            .endPort()
            .endSpec()
            .build();
    assertFalse(
        ServiceProfiles.toProfile(trinoEngine, PREFIX)
            .conf()
            .containsKey("kyuubi.session.engine.trino.connection.url"));
  }

  @Test
  public void portNameNamesAnotherPortForTheEngine() {
    Service hs2 =
        new ServiceBuilder(service("SPARK_SQL", Map.of(PREFIX + "port-name", " thrift ")))
            .editSpec()
            .addNewPort()
            .withName("kyuubi")
            .withPort(10009)
            .endPort()
            .addNewPort()
            .withName("thrift")
            .withPort(10000)
            .endPort()
            .endSpec()
            .build();
    ClusterProfile p = ServiceProfiles.toProfile(hs2, PREFIX);
    assertEquals("analytics.trino.svc", p.engine().get().host());
    assertEquals(10000, p.engine().get().port());
    assertFalse(p.conf().keySet().stream().anyMatch(k -> k.contains("port-name")));

    // Asked for and missing is a typo: the Service is refused, not left without an engine.
    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class,
            () -> profile("SPARK_SQL", Map.of(PREFIX + "port-name", "thirft")));
    assertTrue(missing.getMessage(), missing.getMessage().contains("no port named thirft"));
    assertThrows(
        IllegalArgumentException.class,
        () -> profile("SPARK_SQL", Map.of(PREFIX + "port-name", " ")));
  }
}
