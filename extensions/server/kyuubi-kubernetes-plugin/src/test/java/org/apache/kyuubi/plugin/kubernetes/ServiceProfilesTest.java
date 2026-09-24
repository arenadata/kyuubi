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

  private static Service service(Map<String, String> annotations) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withNamespace("trino")
        .withName("analytics")
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

  private static ClusterProfile profile(Map<String, String> annotations) {
    return ServiceProfiles.toProfile(service(annotations), "kyuubi/");
  }

  @Test
  public void aTrinoServiceIsAProfileWithItsOwnAddress() {
    ClusterProfile p = profile(Map.of("kyuubi/type", "trino"));
    assertEquals("analytics", p.name());
    assertEquals("trino/analytics", p.source());
    assertEquals("TRINO", p.conf().get("kyuubi.engine.type"));
    assertEquals(
        "http://analytics.trino.svc:8080", p.conf().get("kyuubi.engine.trino.connection.url"));
    assertEquals("analytics", p.conf().get("kyuubi.engine.share.level.subdomain"));
    assertTrue(p.users().isEmpty());
    assertFalse(p.isDefault());
    assertTrue(p.allows("anyone"));
  }

  @Test
  public void theBucketsMapAsDeclaredProfilesDo() {
    ClusterProfile p =
        profile(
            Map.of(
                "kyuubi/type", "TRINO",
                "kyuubi/env.TRINO_HOME", "/opt/trino",
                "kyuubi/session.engine.trino.connection.catalog", "hive",
                "kyuubi/conf.kyuubi.engine.trino.memory", "2g",
                "kyuubi/conf.kyuubi.engine.share.level.subdomain", "custom"));
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
            Map.of(
                "kyuubi/type", "JDBC",
                "kyuubi/port", "https",
                "kyuubi/scheme", "https",
                "kyuubi/conf.kyuubi.engine.jdbc.type", "impala",
                "kyuubi/conf.kyuubi.engine.jdbc.connection.url",
                    "jdbc:impala://{host}:{port}/;hive.server2.proxy.user={user}",
                "kyuubi/conf.some.url", "{url}"));
    assertEquals(
        "jdbc:impala://analytics.trino.svc:8443/;hive.server2.proxy.user={user}",
        p.conf().get("kyuubi.engine.jdbc.connection.url"));
    assertEquals("https://analytics.trino.svc:8443", p.conf().get("some.url"));
    assertFalse(
        "a JDBC profile gets no Trino url",
        p.conf().containsKey("kyuubi.engine.trino.connection.url"));
  }

  @Test
  public void nameUsersAndDefaultAreTheServicesToSay() {
    ClusterProfile p =
        profile(
            Map.of(
                "kyuubi/type", "TRINO",
                "kyuubi/profile", "Adhoc Trino",
                "kyuubi/users", "alice, bob,",
                "kyuubi/default", "true"));
    assertEquals("Adhoc Trino", p.name());
    assertEquals("adhoc-trino", p.conf().get("kyuubi.engine.share.level.subdomain"));
    assertEquals(Set.of("alice", "bob"), p.users());
    assertTrue(p.isDefault());
    assertFalse(p.allows("carol"));
  }

  @Test
  public void aServiceThatIsNotAProfileIsRefusedWhole() {
    assertThrows(IllegalArgumentException.class, () -> profile(Map.of("kyuubi/users", "alice")));
    assertThrows(
        IllegalArgumentException.class,
        () -> profile(Map.of("kyuubi/type", "TRINO", "kyuubi/typo.key", "x")));
    assertThrows(
        IllegalArgumentException.class,
        () -> profile(Map.of("kyuubi/type", "TRINO", "kyuubi/port", "grpc")));
    assertThrows(
        IllegalArgumentException.class,
        () -> profile(Map.of("kyuubi/type", "TRINO", "kyuubi/env", "x")));
  }

  @Test
  public void otherPrefixesAreSomebodyElses() {
    ClusterProfile p =
        ServiceProfiles.toProfile(
            service(Map.of("trino-as/type", "ignored", "kyuubi/type", "trino")), "kyuubi/");
    assertEquals(3, p.conf().size());
  }
}
