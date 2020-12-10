/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.ingest.jobs.JobState;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class TableJobStateStats extends Table
{
  private List<JobStateCount> jobStateCounts = new ArrayList<>();

  public TableJobStateStats(Table table)
  {
    super(table);
  }

  @JsonCreator
  public TableJobStateStats(
      @JsonProperty("name") String name,
      @JsonProperty("createdTime") DateTime createdTime,
      @JsonProperty("jobState") JobState jobState,
      @JsonProperty("count") int count
  )
  {
    super(name, createdTime);
    this.addJobState(jobState, count);
  }

  public TableJobStateStats addJobState(@Nullable JobState js, int count)
  {
    if (js != null) {
      jobStateCounts.add(new JobStateCount(js, count));
    }
    return this;
  }

  @JsonProperty
  public List<JobStateCount> getJobStateCounts()
  {
    return jobStateCounts;
  }

  @Override
  public boolean equals(Object o)
  {
    // this extends Table which has implemented equals...
    if (!super.equals(o)) {
      return false;
    }
    TableJobStateStats other = (TableJobStateStats) o;
    return (this.jobStateCounts.equals(other.getJobStateCounts()));
  }

  @Override
  public int hashCode()
  {
    return 31 * this.getPermissions().hashCode() +
           this.jobStateCounts.hashCode() +
           super.hashCode();
  }

  public static class JobStateCount
  {
    private JobState jobState;
    private int count;

    @JsonCreator
    public JobStateCount(
        @JsonProperty JobState js,
        @JsonProperty int count
    )
    {
      this.jobState = js;
      this.count = count;
    }

    @JsonProperty
    public JobState getJobState()
    {
      return jobState;
    }

    @JsonProperty
    public int getCount()
    {
      return count;
    }
  }
}
