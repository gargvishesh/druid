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
import io.imply.druid.ingest.jobs.JobState;
import org.apache.druid.server.security.Action;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class TableInfo
{
  private final String name;
  private final DateTime createdTime;
  private final Set<Action> permissions;
  private final List<JobStateCount> jobStateCounts;

  @JsonCreator
  public TableInfo(
      @JsonProperty("name") String name,
      @JsonProperty("createdTime") DateTime createdTime,
      @JsonProperty("permissions") @Nullable Set<Action> permissions,
      @JsonProperty("jobStateCounts") @Nullable List<JobStateCount> jobStateCounts
  )
  {
    this.name = name;
    this.createdTime = createdTime;
    this.permissions = permissions;
    this.jobStateCounts = jobStateCounts;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  @JsonProperty
  public Set<Action> getPermissions()
  {
    return permissions;
  }

  @JsonProperty
  public List<JobStateCount> getJobStateCounts()
  {
    return jobStateCounts;
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
    TableInfo tableInfo = (TableInfo) o;
    return Objects.equals(name, tableInfo.name)
           && Objects.equals(createdTime, tableInfo.createdTime)
           && Objects.equals(permissions, tableInfo.permissions)
           && Objects.equals(jobStateCounts, tableInfo.jobStateCounts);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, createdTime, permissions, jobStateCounts);
  }

  @Override
  public String toString()
  {
    return "TableInfo{" +
           "name='" + name + '\'' +
           ", createdTime=" + createdTime +
           ", permissions=" + permissions +
           ", jobStateCounts=" + jobStateCounts +
           '}';
  }

  public static class JobStateCount
  {
    private final JobState jobState;
    private final int count;

    @JsonCreator
    public JobStateCount(
        @JsonProperty("jobState") JobState jobState,
        @JsonProperty("count") int count
    )
    {
      this.jobState = jobState;
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

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      JobStateCount that = (JobStateCount) o;
      return count == that.count && jobState == that.jobState;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(jobState, count);
    }

    @Override
    public String toString()
    {
      return "JobStateCount{" +
             "jobState=" + jobState +
             ", count=" + count +
             '}';
    }
  }
}
