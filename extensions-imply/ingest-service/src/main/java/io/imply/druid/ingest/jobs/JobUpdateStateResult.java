/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs;

import javax.annotation.Nullable;

public class JobUpdateStateResult
{
  private final JobState jobState;
  private final JobStatus jobStatus;

  public JobUpdateStateResult(@Nullable JobState jobState, @Nullable JobStatus jobStatus)
  {
    this.jobState = jobState;
    this.jobStatus = jobStatus;
  }

  @Nullable
  public JobState getJobState()
  {
    return jobState;
  }

  @Nullable
  public JobStatus getJobStatus()
  {
    return jobStatus;
  }
}
