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

package org.apache.kyuubi.jdbc.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;

public class ListDiscoveryHelperTest {
  private static final String URI =
      "jdbc:hive2://10.1.0.1:30009,10.1.0.2:30010,[::1]:30011/db;serviceDiscoveryMode=list;user=u";

  @Test
  public void parseUrlPicksOneServerOfTheList() throws Exception {
    JdbcConnectionParams p = Utils.parseURL(URI, new Properties());
    String hostPort = p.getHost() + ":" + p.getPort();
    assertTrue(
        hostPort, hostPort.equals("10.1.0.1:30009") || hostPort.equals("10.1.0.2:30010")
            || p.getHost().contains("::1") && p.getPort() == 30011);
    assertFalse(p.getJdbcUriString().contains(","));
    assertTrue(p.getJdbcUriString().contains(":" + p.getPort()));
  }

  @Test
  public void failoverVisitsEveryServerOnceThenGivesUp() throws Exception {
    JdbcConnectionParams p = Utils.parseURL(URI, new Properties());
    Set<String> visited = new HashSet<>();
    visited.add(p.getHost() + ":" + p.getPort());
    while (Utils.updateConnParamsFromList(p)) {
      assertTrue(visited.add(p.getHost() + ":" + p.getPort()));
    }
    assertEquals(3, visited.size());
  }

  @Test
  public void startsOverWhenRejectedListIsCleared() throws Exception {
    JdbcConnectionParams p = Utils.parseURL(URI, new Properties());
    while (Utils.updateConnParamsFromList(p)) {}
    p.getRejectedAddresses().clear();
    assertTrue(Utils.updateConnParamsFromList(p));
  }

  @Test
  public void missingPortMeansDefault() throws Exception {
    JdbcConnectionParams p =
        Utils.parseURL("jdbc:hive2://a/;serviceDiscoveryMode=list", new Properties());
    assertEquals("a", p.getHost());
    assertEquals(10009, p.getPort());
  }

  @Test
  public void directParamsListHasAllServers() throws Exception {
    JdbcConnectionParams p = Utils.parseURL(URI, new Properties());
    List<JdbcConnectionParams> all = ListDiscoveryHelper.getDirectParamsList(p);
    assertEquals(3, all.size());
  }

  @Test(expected = ZooKeeperHiveClientException.class)
  public void badAddressFails() throws Exception {
    Utils.parseURL("jdbc:hive2://a:1,:2/;serviceDiscoveryMode=list", new Properties());
  }
}
