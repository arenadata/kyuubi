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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Client-side load balancing over a fixed list of servers: {@code serviceDiscoveryMode=list}.
 *
 * <p>The URL carries every server, {@code jdbc:hive2://h1:p1,h2:p2,h3:p3/;serviceDiscoveryMode=list}.
 * The driver picks one at random and connects to it directly. If it does not answer, the next one
 * is tried; once all are rejected the list is tried again (up to {@code retries} times).
 *
 * <p>Meant for clients outside Kubernetes, where pod IPs and headless Service names are not
 * reachable but every pod has its own NodePort or LoadBalancer address.
 */
class ListDiscoveryHelper {

  static boolean isListDiscoveryMode(Map<String, String> sessionConf) {
    return JdbcConnectionParams.SERVICE_DISCOVERY_MODE_LIST.equalsIgnoreCase(
        sessionConf.get(JdbcConnectionParams.SERVICE_DISCOVERY_MODE));
  }

  private static final class Endpoint {
    final String host;
    final int port;

    Endpoint(String host, int port) {
      this.host = host;
      this.port = port;
    }

    String key() {
      return (host.contains(":") ? "[" + host + "]" : host) + ":" + port;
    }
  }

  private static List<Endpoint> parse(String authority) throws ZooKeeperHiveClientException {
    List<Endpoint> result = new ArrayList<>();
    for (String item : authority.split(",")) {
      item = item.trim();
      if (item.isEmpty()) continue;
      URI u;
      try {
        u = URI.create("hive2://" + item);
      } catch (IllegalArgumentException e) {
        throw new ZooKeeperHiveClientException("Bad server address: " + item, e);
      }
      if (u.getHost() == null) {
        throw new ZooKeeperHiveClientException("Bad server address: " + item);
      }
      int port = u.getPort() > 0 ? u.getPort() : Integer.parseInt(Utils.DEFAULT_PORT);
      String h = u.getHost();
      if (h.startsWith("[") && h.endsWith("]")) h = h.substring(1, h.length() - 1);
      Endpoint e = new Endpoint(h, port);
      boolean dup = false;
      for (Endpoint x : result) dup |= x.key().equals(e.key());
      if (!dup) result.add(e);
    }
    if (result.isEmpty()) {
      throw new ZooKeeperHiveClientException("No server address in: " + authority);
    }
    return result;
  }

  /** Pick a random server of the URL's list that has not been rejected yet. */
  static void configureConnParams(JdbcConnectionParams connParams)
      throws ZooKeeperHiveClientException {
    List<Endpoint> candidates = new ArrayList<>();
    for (Endpoint e : parse(connParams.getSuppliedURLAuthority())) {
      if (!connParams.getRejectedAddresses().contains(e.key())) candidates.add(e);
    }
    if (candidates.isEmpty()) {
      throw new ZooKeeperHiveClientException(
          "Tried all servers of the list: " + connParams.getRejectedAddresses());
    }
    Endpoint chosen = candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
    connParams.setCurrentAddress(chosen.key());
    connParams.setHost(chosen.host);
    connParams.setPort(chosen.port);
  }

  static List<JdbcConnectionParams> getDirectParamsList(JdbcConnectionParams connParams)
      throws ZooKeeperHiveClientException {
    List<JdbcConnectionParams> result = new ArrayList<>();
    for (Endpoint e : parse(connParams.getSuppliedURLAuthority())) {
      JdbcConnectionParams p = new JdbcConnectionParams(connParams);
      p.setCurrentAddress(e.key());
      p.setHost(e.host);
      p.setPort(e.port);
      result.add(p);
    }
    Collections.shuffle(result);
    return result;
  }
}
