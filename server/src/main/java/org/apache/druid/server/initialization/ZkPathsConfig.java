/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.utils.ZKPaths;

public class ZkPathsConfig
{
  @JsonProperty
  private String base = "druid";
  @JsonProperty
  private String propertiesPath;
  @JsonProperty
  private String announcementsPath;
  @JsonProperty
  private String liveSegmentsPath;
  @JsonProperty
  private String coordinatorPath;
  @JsonProperty
  private String connectorPath;

  public String getBase()
  {
    return base;
  }

  public String getPropertiesPath()
  {
    return (null == propertiesPath) ? defaultPath("properties") : propertiesPath;
  }

  public String getAnnouncementsPath()
  {
    return (null == announcementsPath) ? defaultPath("announcements") : announcementsPath;
  }

  /**
   * Path to announce served segments on.
   *
   * @deprecated Use HTTP-based segment discovery instead.
   */
  @Deprecated
  public String getLiveSegmentsPath()
  {
    return (null == liveSegmentsPath) ? defaultPath("segments") : liveSegmentsPath;
  }

  public String getCoordinatorPath()
  {
    return (null == coordinatorPath) ? defaultPath("coordinator") : coordinatorPath;
  }

  public String getOverlordPath()
  {
    return defaultPath("overlord");
  }

  public String getConnectorPath()
  {
    return (null == connectorPath) ? defaultPath("connector") : connectorPath;
  }

  public String getInternalDiscoveryPath()
  {
    return defaultPath("internal-discovery");
  }

  public String defaultPath(final String subPath)
  {
    return ZKPaths.makePath(getBase(), subPath);
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other) {
      return false;
    }
    if (this == other) {
      return true;
    }
    if (!(other instanceof ZkPathsConfig)) {
      return false;
    }
    ZkPathsConfig otherConfig = (ZkPathsConfig) other;
    if (this.getBase().equals(otherConfig.getBase()) &&
        this.getAnnouncementsPath().equals(otherConfig.getAnnouncementsPath()) &&
        this.getConnectorPath().equals(otherConfig.getConnectorPath()) &&
        this.getLiveSegmentsPath().equals(otherConfig.getLiveSegmentsPath()) &&
        this.getCoordinatorPath().equals(otherConfig.getCoordinatorPath()) &&
        this.getPropertiesPath().equals(otherConfig.getPropertiesPath())) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    int result = base != null ? base.hashCode() : 0;
    result = 31 * result + (propertiesPath != null ? propertiesPath.hashCode() : 0);
    result = 31 * result + (announcementsPath != null ? announcementsPath.hashCode() : 0);
    result = 31 * result + (liveSegmentsPath != null ? liveSegmentsPath.hashCode() : 0);
    result = 31 * result + (coordinatorPath != null ? coordinatorPath.hashCode() : 0);
    result = 31 * result + (connectorPath != null ? connectorPath.hashCode() : 0);
    return result;
  }
}
