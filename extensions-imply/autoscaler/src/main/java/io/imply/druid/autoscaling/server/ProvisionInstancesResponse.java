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

import java.util.List;
import java.util.Objects;

public class ProvisionInstancesResponse
{
  private final List<String> instanceIds;

  @JsonCreator
  public ProvisionInstancesResponse(
      @JsonProperty("instanceIds") List<String> instanceIds
  )
  {
    this.instanceIds = instanceIds;
  }

  @JsonProperty
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
    ProvisionInstancesResponse that = (ProvisionInstancesResponse) o;
    return Objects.equals(instanceIds, that.instanceIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(instanceIds);
  }
}
