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

import java.util.List;
import java.util.Objects;

public class IngestJobsResponse
{
  private final List<IngestJobInfo> jobs;

  @JsonCreator
  public IngestJobsResponse(
      @JsonProperty("jobs") List<IngestJobInfo> jobs
  )
  {
    this.jobs = jobs;
  }

  @JsonProperty
  public List<IngestJobInfo> getJobs()
  {
    return jobs;
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
    IngestJobsResponse that = (IngestJobsResponse) o;
    return Objects.equals(jobs, that.jobs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(jobs);
  }

  @Override
  public String toString()
  {
    return "IngestJobsResponse{" +
           "jobs=" + jobs +
           '}';
  }
}
