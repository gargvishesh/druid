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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.talaria.indexing.TalariaCountersSnapshot;

import javax.annotation.Nullable;

public class TalariaTaskReportPayload
{
  private final TalariaStatusReport status;

  @Nullable
  private final TalariaStagesReport stages;

  @Nullable
  private final TalariaCountersSnapshot counters;

  @Nullable
  private final TalariaResultsReport results;

  @JsonCreator
  public TalariaTaskReportPayload(
      @JsonProperty("status") TalariaStatusReport status,
      @JsonProperty("stages") @Nullable TalariaStagesReport stages,
      @JsonProperty("counters") @Nullable TalariaCountersSnapshot counters,
      @JsonProperty("results") @Nullable TalariaResultsReport results
  )
  {
    this.status = status;
    this.stages = stages;
    this.counters = counters;
    this.results = results;
  }

  @JsonProperty
  public TalariaStatusReport getStatus()
  {
    return status;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public TalariaStagesReport getStages()
  {
    return stages;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public TalariaCountersSnapshot getCounters()
  {
    return counters;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public TalariaResultsReport getResults()
  {
    return results;
  }
}
