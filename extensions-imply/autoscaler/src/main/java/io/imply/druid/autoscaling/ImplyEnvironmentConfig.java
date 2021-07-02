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
public class ImplyEnvironmentConfig
{
  private final String implyAddress;
  private final String clusterId;

  @JsonCreator
  public ImplyEnvironmentConfig(
      @JsonProperty("implyAddress") String implyAddress,
      @JsonProperty("clusterId") String clusterId
  )
  {
    this.implyAddress = Preconditions.checkNotNull(implyAddress, "implyAddress must be not null");
    this.clusterId = Preconditions.checkNotNull(clusterId, "clusterId must be not null");
  }

  @JsonProperty
  public String getImplyAddress()
  {
    return implyAddress;
  }

  @JsonProperty
  public String getClusterId()
  {
    return clusterId;
  }

  @Override
  public String toString()
  {
    return "ImplyEnvironmentConfig{" +
           "implyAddress='" + implyAddress + '\'' +
           ", clusterId='" + clusterId + '\'' +
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
    ImplyEnvironmentConfig that = (ImplyEnvironmentConfig) o;
    return Objects.equals(implyAddress, that.implyAddress) &&
           Objects.equals(clusterId, that.clusterId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(implyAddress, clusterId);
  }
}
