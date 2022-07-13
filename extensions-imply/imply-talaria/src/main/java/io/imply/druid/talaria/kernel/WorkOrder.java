/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.input.InputSlice;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class WorkOrder
{
  private final QueryDefinition queryDefinition;
  private final int stageNumber;
  private final int workerNumber;
  private final List<InputSlice> workerInputs;
  private final ExtraInfoHolder<?> extraInfoHolder;

  @JsonCreator
  @SuppressWarnings("rawtypes")
  public WorkOrder(
      @JsonProperty("query") final QueryDefinition queryDefinition,
      @JsonProperty("stage") final int stageNumber,
      @JsonProperty("worker") final int workerNumber,
      @JsonProperty("input") final List<InputSlice> workerInputs,
      @JsonProperty("extra") @Nullable final ExtraInfoHolder extraInfoHolder
  )
  {
    this.queryDefinition = Preconditions.checkNotNull(queryDefinition, "queryDefinition");
    this.stageNumber = stageNumber;
    this.workerNumber = workerNumber;
    this.workerInputs = Preconditions.checkNotNull(workerInputs, "workerInputs");
    this.extraInfoHolder = extraInfoHolder;
  }

  @JsonProperty("query")
  public QueryDefinition getQueryDefinition()
  {
    return queryDefinition;
  }

  @JsonProperty("stage")
  public int getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty("worker")
  public int getWorkerNumber()
  {
    return workerNumber;
  }

  @JsonProperty("input")
  public List<InputSlice> getInputs()
  {
    return workerInputs;
  }

  @Nullable
  @JsonProperty("extra")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  ExtraInfoHolder<?> getExtraInfoHolder()
  {
    return extraInfoHolder;
  }

  @Nullable
  public Object getExtraInfo()
  {
    return extraInfoHolder != null ? extraInfoHolder.getExtraInfo() : null;
  }

  public StageDefinition getStageDefinition()
  {
    return queryDefinition.getStageDefinition(stageNumber);
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
    WorkOrder workOrder = (WorkOrder) o;
    return stageNumber == workOrder.stageNumber
           && workerNumber == workOrder.workerNumber
           && Objects.equals(queryDefinition, workOrder.queryDefinition)
           && Objects.equals(workerInputs, workOrder.workerInputs)
           && Objects.equals(extraInfoHolder, workOrder.extraInfoHolder);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryDefinition, stageNumber, workerInputs, workerNumber, extraInfoHolder);
  }

  @Override
  public String toString()
  {
    return "WorkOrder{" +
           "queryDefinition=" + queryDefinition +
           ", stageNumber=" + stageNumber +
           ", workerNumber=" + workerNumber +
           ", workerInputs=" + workerInputs +
           ", extraInfoHolder=" + extraInfoHolder +
           '}';
  }
}
