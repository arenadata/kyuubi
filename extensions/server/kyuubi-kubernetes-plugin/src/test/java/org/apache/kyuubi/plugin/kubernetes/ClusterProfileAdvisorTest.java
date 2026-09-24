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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kyuubi.KyuubiSQLException;
import org.apache.kyuubi.config.KyuubiConf;
import org.junit.Test;

public class ClusterProfileAdvisorTest {

  private static final ClusterProfile ANALYTICS =
      new ClusterProfile(
          "analytics",
          "trino/analytics",
          Set.of("alice"),
          false,
          Map.of(
              "kyuubi.engine.type",
              "TRINO",
              "kyuubi.engine.trino.connection.url",
              "http://a:8080"));
  private static final ClusterProfile SHARED =
      new ClusterProfile(
          "shared",
          "trino/shared",
          Set.of(),
          true,
          Map.of(
              "kyuubi.engine.type", "JDBC",
              "kyuubi.engine.jdbc.connection.url",
                  "jdbc:impala://s:21050/;hive.server2.proxy.user={user}"));

  private static ClusterProfileAdvisor advisor(ClusterProfile... profiles) {
    return new ClusterProfileAdvisor(() -> List.of(profiles), Set.of("declared"));
  }

  @Test
  public void aRequestedServiceProfileIsAppliedWithTheUserFilledIn() {
    Map<String, String> overlay =
        advisor(ANALYTICS, SHARED).getConfOverlay("bob", Map.of("kyuubi.engine.profile", "shared"));
    assertEquals("JDBC", overlay.get("kyuubi.engine.type"));
    assertEquals(
        "jdbc:impala://s:21050/;hive.server2.proxy.user=bob",
        overlay.get("kyuubi.engine.jdbc.connection.url"));
  }

  @Test
  public void aRequestedDeclaredProfileIsLeftToTheServer() {
    assertTrue(
        advisor(ANALYTICS)
            .getConfOverlay("alice", Map.of("kyuubi.engine.profile", "declared"))
            .isEmpty());
  }

  @Test
  public void anUnknownProfileFailsTheSessionNamingWhatExists() {
    KyuubiSQLException e =
        assertThrows(
            KyuubiSQLException.class,
            () ->
                advisor(ANALYTICS, SHARED)
                    .getConfOverlay("alice", Map.of("kyuubi.engine.profile", "nope")));
    assertTrue(e.getMessage(), e.getMessage().contains("[analytics, shared]"));
    assertTrue(e.getMessage(), e.getMessage().contains("[declared]"));
  }

  @Test
  public void aProfileNamingUsersIsTheirsAlone() {
    assertThrows(
        KyuubiSQLException.class,
        () ->
            advisor(ANALYTICS).getConfOverlay("bob", Map.of("kyuubi.engine.profile", "analytics")));
    assertEquals(
        "TRINO",
        advisor(ANALYTICS)
            .getConfOverlay("alice", Map.of("kyuubi.engine.profile", "analytics"))
            .get("kyuubi.engine.type"));
  }

  @Test
  public void withoutARequestTheUsersProfileThenTheDefaultApplies() {
    assertEquals(
        "TRINO",
        advisor(ANALYTICS, SHARED).getConfOverlay("alice", Map.of()).get("kyuubi.engine.type"));
    assertEquals(
        "JDBC",
        advisor(ANALYTICS, SHARED).getConfOverlay("carol", Map.of()).get("kyuubi.engine.type"));
    assertTrue(
        "nothing declared for anyone: the server's own resolution stands",
        advisor(ANALYTICS).getConfOverlay("carol", Map.of()).isEmpty());
  }

  @Test
  public void settingsAndDeclaredProfilesComeFromTheServersConfiguration() {
    KyuubiConf conf = new KyuubiConf(false);
    conf.set(ClusterProfileAdvisor.NAMESPACES_KEY, "trino, impala");
    conf.set("kyuubi.engine.profile.etl.type", "TRINO");
    conf.set("kyuubi.engine.profile.etl.session.engine.trino.connection.catalog", "hive");
    conf.set("kyuubi.engine.profile.dw.type", "JDBC");
    KubernetesClusterCatalog.Settings s = ClusterProfileAdvisor.settingsFrom(conf);
    assertEquals(List.of("trino", "impala"), s.namespaces());
    assertEquals(ClusterProfileAdvisor.DEFAULT_LABEL_SELECTOR, s.labelSelector());
    assertEquals("kyuubi/", s.annotationPrefix());
    assertEquals(Set.of("etl", "dw"), ClusterProfileAdvisor.declaredProfilesIn(conf));
  }
}
