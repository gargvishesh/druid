/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

public class ProvisionInstancesResponse
{
  private final List<ProvisionInstanceResponse> instances;

  @JsonCreator
  public ProvisionInstancesResponse(
      @JsonProperty("instances") List<ProvisionInstanceResponse> instances
  )
  {
    this.instances = instances;
  }

  @JsonProperty
  public List<ProvisionInstanceResponse> getInstances()
  {
    return instances;
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
    ProvisionInstancesResponse response = (ProvisionInstancesResponse) o;
    return Objects.equals(instances, response.instances);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(instances);
  }

  public static class ProvisionInstanceResponse
  {
    private final String version;
    private final List<String> instanceIds;

    @JsonCreator
    public ProvisionInstanceResponse(
        @JsonProperty("version") String version,
        @JsonProperty("instanceIds") List<String> instanceIds
    )
    {
      this.version = Preconditions.checkNotNull(version, "version must be not null");
      this.instanceIds = instanceIds;
    }

    @JsonProperty("version")
    public String getVersion()
    {
      return version;
    }

    @JsonProperty("instanceIds")
    public List<String> getInstanceIds()
    {
      return instanceIds;
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
      ProvisionInstanceResponse that = (ProvisionInstanceResponse) o;
      return Objects.equals(version, that.version) &&
             Objects.equals(instanceIds, that.instanceIds);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(version, instanceIds);
    }
  }
}
