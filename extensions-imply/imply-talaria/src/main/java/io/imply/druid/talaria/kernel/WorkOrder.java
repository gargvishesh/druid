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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

public class WorkOrder
{
  private final QueryDefinition queryDefinition;
  private final int stageNumber;
  private final ReadablePartitions inputPartitions;
  private final int workerNumber;
  private final ExtraInfoHolder<?> extraInfoHolder;

  @JsonCreator
  @SuppressWarnings("rawtypes")
  public WorkOrder(
      @JsonProperty("queryDefinition") final QueryDefinition queryDefinition,
      @JsonProperty("stageNumber") final int stageNumber,
      @JsonProperty("workerNumber") final int workerNumber,
      @JsonProperty("inputPartitions") final ReadablePartitions inputPartitions,
      @JsonProperty("extraInfo") final ExtraInfoHolder extraInfoHolder
  )
  {
    this.queryDefinition = Preconditions.checkNotNull(queryDefinition, "queryDefinition");
    this.stageNumber = stageNumber;
    this.inputPartitions = Preconditions.checkNotNull(inputPartitions, "inputPartitions");
    this.workerNumber = workerNumber;
    this.extraInfoHolder = Preconditions.checkNotNull(extraInfoHolder, "extraInfo");
  }

  @JsonProperty
  public QueryDefinition getQueryDefinition()
  {
    return queryDefinition;
  }

  @JsonProperty
  public int getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty
  public ReadablePartitions getInputPartitions()
  {
    return inputPartitions;
  }

  @JsonProperty
  public int getWorkerNumber()
  {
    return workerNumber;
  }

  @JsonProperty("extraInfo")
  public ExtraInfoHolder<?> getExtraInfoHolder()
  {
    return extraInfoHolder;
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
           && Objects.equals(inputPartitions, workOrder.inputPartitions)
           && Objects.equals(extraInfoHolder, workOrder.extraInfoHolder);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryDefinition, stageNumber, inputPartitions, workerNumber, extraInfoHolder);
  }

  @Override
  public String toString()
  {
    return "WorkOrder{" +
           "queryDefinition=" + queryDefinition +
           ", stageNumber=" + stageNumber +
           ", inputPartitions=" + inputPartitions +
           ", workerNumber=" + workerNumber +
           ", extraInfoHolder=" + extraInfoHolder +
           '}';
  }
}
