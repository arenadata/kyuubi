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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An engine profile declared by a Service rather than by {@code kyuubi-defaults.conf}.
 *
 * <p>{@code conf} is the session configuration it applies, already mapped to Kyuubi keys the way
 * {@code EngineProfileRegistry} maps a declared profile; {@code {user}} in a value stands for the
 * session user.
 */
public final class ClusterProfile {

  private final String name;
  private final String source;
  private final Set<String> users;
  private final boolean isDefault;
  private final Map<String, String> conf;

  /**
   * @param name the profile name a session asks for with {@code kyuubi.engine.profile}
   * @param source the Service it was read from, {@code namespace/name}
   * @param users who may use it; empty means anyone who asks for it by name
   * @param isDefault whether users who ask for nothing land here
   * @param conf what it applies to a session
   */
  public ClusterProfile(
      String name, String source, Set<String> users, boolean isDefault, Map<String, String> conf) {
    this.name = name;
    this.source = source;
    this.users = Collections.unmodifiableSet(new LinkedHashSet<>(users));
    this.isDefault = isDefault;
    this.conf = Collections.unmodifiableMap(new LinkedHashMap<>(conf));
  }

  public String name() {
    return name;
  }

  public String source() {
    return source;
  }

  public Set<String> users() {
    return users;
  }

  public boolean isDefault() {
    return isDefault;
  }

  public Map<String, String> conf() {
    return conf;
  }

  public boolean allows(String user) {
    return users.isEmpty() || users.contains(user);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ClusterProfile)) {
      return false;
    }
    ClusterProfile other = (ClusterProfile) o;
    return name.equals(other.name)
        && source.equals(other.source)
        && users.equals(other.users)
        && isDefault == other.isDefault
        && conf.equals(other.conf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, source, users, isDefault, conf);
  }

  @Override
  public String toString() {
    return name + " (" + source + ")";
  }
}
