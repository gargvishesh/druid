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

import java.util.Objects;

public class ProvisionInstancesRequest
{
  private final String version;
  private final int numToCreate;

  @JsonCreator
  public ProvisionInstancesRequest(
      @JsonProperty("version") String version,
      @JsonProperty("numToCreate") int numToCreate
  )
  {
    this.version = Preconditions.checkNotNull(version, "version must be not null");
    this.numToCreate = numToCreate;
  }

  @JsonProperty("version")
  public String getVersion()
  {
    return version;
  }

  @JsonProperty("numToCreate")
  public int getNumToCreate()
  {
    return numToCreate;
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
    ProvisionInstancesRequest request = (ProvisionInstancesRequest) o;
    return numToCreate == request.numToCreate &&
           Objects.equals(version, request.version);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(version, numToCreate);
  }
}
