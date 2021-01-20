/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.util.Objects;

public class StageBatchAppendPushIngestJobResponse
{
  private final String jobId;
  private final URI dropoffUri;

  @JsonCreator
  public StageBatchAppendPushIngestJobResponse(
      @JsonProperty("jobId") String jobId,
      @JsonProperty("dropoffUri") URI dropoffUri)
  {
    this.jobId = jobId;
    this.dropoffUri = dropoffUri;
  }

  @JsonProperty("jobId")
  public String getJobId()
  {
    return jobId;
  }

  @JsonProperty("dropoffUri")
  public URI getDropoffUri()
  {
    return dropoffUri;
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
    StageBatchAppendPushIngestJobResponse that = (StageBatchAppendPushIngestJobResponse) o;
    return Objects.equals(jobId, that.jobId) && Objects.equals(dropoffUri, that.dropoffUri);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(jobId, dropoffUri);
  }

  @Override
  public String toString()
  {
    return "StageBatchAppendPushIngestJobResponse{" +
           "jobId='" + jobId + '\'' +
           ", dropoffUri=" + dropoffUri +
           '}';
  }
}
