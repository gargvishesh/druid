/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;

public class TaskReportMSQDestination implements MSQDestination
{
  public static final TaskReportMSQDestination INSTANCE = new TaskReportMSQDestination();
  static final String TYPE = "taskReport";

  private TaskReportMSQDestination()
  {
    // Singleton.
  }

  @JsonCreator
  public static TaskReportMSQDestination instance()
  {
    return INSTANCE;
  }

  @Override
  public String toString()
  {
    return "TaskReportMSQDestination{}";
  }
}
