/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs.status;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.ingest.jobs.JobStatus;

import java.util.Objects;

public class FailedJobStatus implements JobStatus
{
  private final String errorMessage;

  @JsonCreator
  public FailedJobStatus(
      @JsonProperty("message") String errorMessage
  )
  {
    this.errorMessage = errorMessage;
  }

  @Override
  public String getUserFacingMessage()
  {
    return errorMessage;
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
    FailedJobStatus that = (FailedJobStatus) o;
    return Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(errorMessage);
  }
}
