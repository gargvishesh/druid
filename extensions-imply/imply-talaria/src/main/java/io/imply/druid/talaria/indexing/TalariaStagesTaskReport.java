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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.indexing.common.TaskReport;

@JsonTypeName(TalariaStagesTaskReport.REPORT_KEY)
public class TalariaStagesTaskReport implements TaskReport
{
  public static final String REPORT_KEY = "talariaStages";

  private final String taskId;
  private final TalariaStagesTaskReportPayload payload;

  // TODO(gianm): include 'last updated' times at various places (help detect stale workers)
  @JsonCreator
  public TalariaStagesTaskReport(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("payload") final TalariaStagesTaskReportPayload payload
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
