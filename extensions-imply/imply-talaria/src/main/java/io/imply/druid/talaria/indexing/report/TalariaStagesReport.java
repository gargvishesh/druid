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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.controller.ControllerStagePhase;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TalariaStagesReport
{
  private final List<Stage> stages;

  @JsonCreator
  public TalariaStagesReport(@JsonProperty("stages") final List<Stage> stages)
  {
    this.stages = Preconditions.checkNotNull(stages, "stages");
  }

  public static TalariaStagesReport create(
      final QueryDefinition queryDef,
      final Map<Integer, ControllerStagePhase> stagePhaseMap,
      final Map<Integer, Interval> stageRuntimeMap,
      final Map<Integer, Integer> stageWorkerCountMap,
      final Map<Integer, Integer> stagePartitionCountMap
  )
  {
    final List<Stage> stages = new ArrayList<>();

    final int[] stageNumbers =
        queryDef.getStageDefinitions().stream().mapToInt(StageDefinition::getStageNumber).toArray();
    Arrays.sort(stageNumbers);

    for (final int stageNumber : stageNumbers) {
      final StageDefinition stageDef = queryDef.getStageDefinition(stageNumber);

      final int workerCount = stageWorkerCountMap.getOrDefault(stageNumber, 0);
      final int partitionCount = stagePartitionCountMap.getOrDefault(stageNumber, 0);
      final Interval stageRuntimeInterval = stageRuntimeMap.get(stageNumber);
      final DateTime stageStartTime = stageRuntimeInterval == null ? null : stageRuntimeInterval.getStart();
      final long stageDuration = stageRuntimeInterval == null ? 0 : stageRuntimeInterval.toDurationMillis();

      final Stage stage = new Stage(
          stageNumber,
          stageDef,
          stagePhaseMap.get(stageNumber),
          workerCount,
          partitionCount,
          stageStartTime,
          stageDuration
      );
      stages.add(stage);
    }

    return new TalariaStagesReport(stages);
  }

  @JsonValue
  public List<Stage> getStages()
  {
    return stages;
  }

  public static class Stage
  {
    private final int stageNumber;
    private final StageDefinition stageDef;
    @Nullable
    private final ControllerStagePhase phase;
    private final int workerCount;
    private final int partitionCount;
    private final DateTime startTime;
    private final long duration;

    @JsonCreator
    private Stage(
        @JsonProperty("stageNumber") final int stageNumber,
        @JsonProperty("definition") final StageDefinition stageDef,
        @JsonProperty("phase") @Nullable final ControllerStagePhase phase,
        @JsonProperty("workerCount") final int workerCount,
        @JsonProperty("partitionCount") final int partitionCount,
        @JsonProperty("startTime") @Nullable final DateTime startTime,
        @JsonProperty("duration") final long duration
    )
    {
      this.stageNumber = stageNumber;
      this.stageDef = stageDef;
      this.phase = phase;
      this.workerCount = workerCount;
      this.partitionCount = partitionCount;
      this.startTime = startTime;
      this.duration = duration;
    }

    @JsonProperty
    public int getStageNumber()
    {
      return stageNumber;
    }

    @JsonProperty("definition")
    public StageDefinition getStageDefinition()
    {
      return stageDef;
    }

    @Nullable
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ControllerStagePhase getPhase()
    {
      // Null if the stage has not yet been started.
      return phase;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getWorkerCount()
    {
      return workerCount;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public int getPartitionCount()
    {
      return partitionCount;
    }

    @JsonProperty("sort")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isSorting()
    {
      // Field written out, but not read, because it is derived from "definition".
      return stageDef.doesSortDuringShuffle();
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public DateTime getStartTime()
    {
      return startTime;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long getDuration()
    {
      return duration;
    }
  }
}
