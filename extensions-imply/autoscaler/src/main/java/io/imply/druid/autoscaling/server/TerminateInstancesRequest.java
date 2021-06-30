/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling.server;

import java.util.List;
import java.util.Objects;

public class TerminateInstancesRequest
{
  private final List<String> instances;

  @JsonCreator
  public TerminateInstancesRequest(
      @JsonProperty("instances") List<String> instances
  )
  {
    this.instances = instances;
  }

  @JsonProperty
  public List<String> getInstances()
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
    TerminateInstancesRequest that = (TerminateInstancesRequest) o;
    return Objects.equals(instances, that.instances);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(instances);
  }
}
