/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

import com.sun.istack.internal.Nullable;

import java.util.List;
import java.util.Objects;

public class ProvisionInstancesRequest
{
  private final List<Instance> instances;

  @JsonCreator
  public ProvisionInstancesRequest(
      @JsonProperty("instances") List<Instance> instances
  )
  {
    this.instances = instances;
  }

  @JsonProperty
  public List<Instance> getInstances()
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
    ProvisionInstancesRequest that = (ProvisionInstancesRequest) o;
    return Objects.equals(instances, that.instances);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(instances);
  }

  public static class Instance
  {
    private final String version;
    private final Integer numInstances;

    @JsonCreator
    public Instance(
        @JsonProperty("version") String version,
        @JsonProperty("numInstances") @Nullable Integer numInstances
    )
    {
      this.version = Preconditions.checkNotNull(version, "version must be not null");
      this.numInstances = numInstances == null ? 1 : numInstances;
    }

    @JsonProperty("version")
    public String getVersion()
    {
      return version;
    }

    @JsonProperty("numInstances")
    public Integer getNumInstances()
    {
      return numInstances;
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
      ProvisionInstancesRequest that = (ProvisionInstancesRequest) o;
      return Objects.equals(version, that.version) &&
             Objects.equals(numInstances, that.numInstances);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(version, numInstances);
    }
  }
}
