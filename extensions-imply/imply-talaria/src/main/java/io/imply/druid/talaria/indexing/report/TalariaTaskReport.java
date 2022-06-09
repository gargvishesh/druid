/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.indexing.common.TaskReport;

@JsonTypeName(TalariaTaskReport.REPORT_KEY)
public class TalariaTaskReport implements TaskReport
{
  public static final String REPORT_KEY = "multiStageQuery";

  private final String taskId;
  private final TalariaTaskReportPayload payload;

  @JsonCreator
  public TalariaTaskReport(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("payload") final TalariaTaskReportPayload payload
  )
  {
    this.taskId = taskId;
    this.payload = payload;
  }

  @Override
  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @Override
  public String getReportKey()
  {
    return REPORT_KEY;
  }

  @Override
  @JsonProperty
  public Object getPayload()
  {
    return payload;
  }
}
