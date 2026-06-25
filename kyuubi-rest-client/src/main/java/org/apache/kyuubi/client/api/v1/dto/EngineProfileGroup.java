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

package org.apache.kyuubi.client.api.v1.dto;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/** Active engine instances grouped by engine profile name and engine type. */
public class EngineProfileGroup {

  private String profile;
  private String engineType;
  private String version;
  private int instanceCount;
  private String status;
  private List<Engine> engines;

  public EngineProfileGroup() {}

  public EngineProfileGroup(
      String profile,
      String engineType,
      String version,
      int instanceCount,
      String status,
      List<Engine> engines) {
    this.profile = profile;
    this.engineType = engineType;
    this.version = version;
    this.instanceCount = instanceCount;
    this.status = status;
    this.engines = engines;
  }

  public String getProfile() {
    return profile;
  }

  public void setProfile(String profile) {
    this.profile = profile;
  }

  public String getEngineType() {
    return engineType;
  }

  public void setEngineType(String engineType) {
    this.engineType = engineType;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public int getInstanceCount() {
    return instanceCount;
  }

  public void setInstanceCount(int instanceCount) {
    this.instanceCount = instanceCount;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public List<Engine> getEngines() {
    if (null == engines) {
      return Collections.emptyList();
    }
    return engines;
  }

  public void setEngines(List<Engine> engines) {
    this.engines = engines;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EngineProfileGroup that = (EngineProfileGroup) o;
    return getInstanceCount() == that.getInstanceCount()
        && Objects.equals(getProfile(), that.getProfile())
        && Objects.equals(getEngineType(), that.getEngineType())
        && Objects.equals(getVersion(), that.getVersion())
        && Objects.equals(getStatus(), that.getStatus())
        && Objects.equals(getEngines(), that.getEngines());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getProfile(), getEngineType(), getVersion(), getInstanceCount(), getStatus(), getEngines());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
