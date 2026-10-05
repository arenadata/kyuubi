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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kyuubi.KyuubiSQLException;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.ha.client.ServiceNodeInfo;
import org.junit.Before;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConverters;

public class KubernetesDiscoveryClientTest {

  private static final String V = "kyuubi_1.11.1.2-4.4.0-0";

  private static ClusterProfile profile(
      String name, String type, Optional<ClusterProfile.Engine> engine) {
    return new ClusterProfile(
        name,
        "engines/" + name,
        Set.of(),
        false,
        Map.of("kyuubi.engine.type", type, "kyuubi.engine.share.level.subdomain", name),
        engine);
  }

  private static final ClusterProfile SPARK =
      profile(
          "spark4",
          "SPARK_SQL",
          Optional.of(new ClusterProfile.Engine("spark4.engines.svc", 10009)));
  private static final ClusterProfile TRINO =
      profile(
          "trino",
          "TRINO",
          Optional.of(new ClusterProfile.Engine("trino-engine.engines.svc", 10010)));
  private static final ClusterProfile NO_ENGINE = profile("impala", "JDBC", Optional.empty());

  private KubernetesDiscoveryClient client;

  @Before
  public void setUp() {
    KubernetesDiscoveryClient.reset();
    client =
        new KubernetesDiscoveryClient(
            new KyuubiConf(false), () -> List.of(SPARK, TRINO, NO_ENGINE));
  }

  @Test
  public void anEngineSpaceIsServedByTheServiceOfItsProfile() {
    assertEquals(
        "spark4.engines.svc",
        client.getServerHost("/" + V + "_USER_SPARK_SQL/alice/spark4").get()._1());
    assertEquals(
        10009, client.getServerHost("/" + V + "_SERVER_SPARK_SQL/kyuubi/spark4").get()._2());
    assertEquals(10010, client.getServerHost("/" + V + "_GROUP_TRINO/analysts/trino").get()._2());
    assertEquals(
        "the host part SERVER_LOCAL adds is not part of the profile",
        "spark4.engines.svc",
        client
            .getServerHost("/" + V + "_SERVER_LOCAL_SPARK_SQL/kyuubi/10.0.0.7_spark4")
            .get()
            ._1());
    assertEquals(
        10009,
        client.getEngineByRefId("/" + V + "_USER_SPARK_SQL/alice/spark4", "any-ref").get()._2());
  }

  @Test
  public void aSpaceNoServiceServesIsRefusedRatherThanLaunched() {
    KyuubiSQLException unknown =
        assertThrows(
            KyuubiSQLException.class,
            () -> client.getServerHost("/" + V + "_USER_SPARK_SQL/alice/default"));
    assertTrue(
        unknown.getMessage(), unknown.getMessage().contains("no Service declares a profile"));
    assertTrue(unknown.getMessage(), unknown.getMessage().contains("not by Kyuubi"));

    KyuubiSQLException wrongType =
        assertThrows(
            KyuubiSQLException.class,
            () -> client.getServerHost("/" + V + "_USER_TRINO/alice/spark4"));
    assertTrue(wrongType.getMessage().contains("no Service declares a profile"));

    KyuubiSQLException noPort =
        assertThrows(
            KyuubiSQLException.class,
            () -> client.getServerHost("/" + V + "_USER_JDBC/alice/impala"));
    assertTrue(noPort.getMessage(), noPort.getMessage().contains("has no port named 'kyuubi'"));

    assertThrows(
        "a connection-level space names a connection, not a profile",
        KyuubiSQLException.class,
        () -> client.getServerHost("/" + V + "_CONNECTION_SPARK_SQL/alice/5c1f-ref-id"));
  }

  @Test
  public void theServiceIsTheOneNodeOfItsSpaceAndIsNotDeleted() {
    String space = "/" + V + "_USER_SPARK_SQL/alice/spark4";
    List<ServiceNodeInfo> nodes =
        JavaConverters.seqAsJavaList(client.getServiceNodesInfo(space, Option.empty(), false));
    assertEquals(1, nodes.size());
    assertEquals("service=engines/spark4", nodes.get(0).nodeName());
    assertEquals(10009, nodes.get(0).port());

    // The server deletes a node it cannot connect to; the engine is its operator's.
    client.delete(space + "/" + nodes.get(0).nodeName(), false);
    assertEquals(1, client.getServiceNodesInfo(space, Option.empty(), false).size());
    assertTrue(
        client
            .getServiceNodesInfo("/" + V + "_USER_SPARK_SQL/alice/none", Option.empty(), true)
            .isEmpty());
  }

  @Test
  public void countersLocksAndPathsAreKeptInMemoryAcrossClients() {
    KubernetesDiscoveryClient other =
        new KubernetesDiscoveryClient(new KyuubiConf(false), () -> List.of());
    assertEquals(0, client.getAndIncrement("/seq/pool", 1));
    assertEquals(1, other.getAndIncrement("/seq/pool", 1));

    assertEquals("yes", client.tryWithLock("/locks/x", 100, () -> "yes"));

    String node = client.create("/a/b/n-", "PERSISTENT_SEQUENTIAL", true);
    assertTrue(other.pathExists(node));
    assertTrue(other.pathExists("/a/b"));
    assertFalse(other.pathNonExists("/a/b/n", true));
    assertEquals(1, JavaConverters.seqAsJavaList(other.getChildren("/a/b")).size());
    client.delete("/a", true);
    assertFalse(other.pathExists(node));
  }
}
