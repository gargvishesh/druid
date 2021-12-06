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

public class TaskReportTalariaDestination implements TalariaDestination
{
  public static final TaskReportTalariaDestination INSTANCE = new TaskReportTalariaDestination();
  static final String TYPE = "taskReport";

  private TaskReportTalariaDestination()
  {
    // Singleton.
  }

  @JsonCreator
  public static TaskReportTalariaDestination instance()
  {
    return INSTANCE;
  }
}
