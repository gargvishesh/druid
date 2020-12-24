/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import io.imply.druid.ingest.jobs.JobState;
import org.apache.druid.java.util.common.StringUtils;

public class JobScheduleException extends RuntimeException
{
  public JobScheduleException(String jobId, JobState jobState)
  {
    super(StringUtils.format("Cannot schedule job [%s] because it is in [%s] state", jobId, jobState));
  }
}
