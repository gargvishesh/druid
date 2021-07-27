/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Environment configurations for the Imply Manager managing this Druid cluster
 */
public class ImplyManagerEnvironmentConfig
{
  private final String implyManagerAddress;
  private final String clusterId;
  private final boolean useHttps;

  @JsonCreator
  public ImplyManagerEnvironmentConfig(
      @JsonProperty("implyManagerAddress") String implyManagerAddress,
      @JsonProperty("clusterId") String clusterId,
      @JsonProperty("useHttps") boolean useHttps
  )
  {
    this.implyManagerAddress = Preconditions.checkNotNull(implyManagerAddress, "implyManagerAddress must be not null");
    this.clusterId = Preconditions.checkNotNull(clusterId, "clusterId must be not null");
    this.useHttps = useHttps;
  }

  @JsonProperty
  public String getImplyManagerAddress()
  {
    return implyManagerAddress;
  }

  @JsonProperty
  public String getClusterId()
  {
    return clusterId;
  }

  @JsonProperty
  public boolean isUseHttps()
  {
    return useHttps;
  }

  @Override
  public String toString()
  {
    return "ImplyManagerEnvironmentConfig{" +
           "implyManagerAddress='" + implyManagerAddress + '\'' +
           ", clusterId='" + clusterId + '\'' +
           ", useHttps=" + useHttps +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ImplyManagerEnvironmentConfig that = (ImplyManagerEnvironmentConfig) o;
    return useHttps == that.useHttps &&
           Objects.equals(implyManagerAddress, that.implyManagerAddress) &&
           Objects.equals(clusterId, that.clusterId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(implyManagerAddress, clusterId, useHttps);
  }
}
