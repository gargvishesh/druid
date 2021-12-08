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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.ShuffleSpec;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.controller.ControllerStagePhase;
import org.apache.druid.query.Query;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TalariaStagesTaskReportPayload
{
  private final List<Stage> stages;

  @JsonCreator
  public TalariaStagesTaskReportPayload(@JsonProperty("stages") final List<Stage> stages)
  {
    this.stages = Preconditions.checkNotNull(stages, "stages");
  }

  public static TalariaStagesTaskReportPayload create(
      final QueryDefinition queryDef,
      final Map<Integer, ControllerStagePhase> stagePhaseMap,
      final Map<Integer, Interval> stageRuntimeMap,
      final Map<Integer, Query<?>> stageQueryMap,
      final TalariaCountersSnapshot counters
  )
  {
    final List<Stage> stages = new ArrayList<>();

    final int[] stageNumbers =
        queryDef.getStageDefinitions().stream().mapToInt(StageDefinition::getStageNumber).toArray();
    Arrays.sort(stageNumbers);

    for (final int stageNumber : stageNumbers) {
      final StageDefinition stageDef = queryDef.getStageDefinition(stageNumber);
      final TalariaCountersSnapshot countersForStage = counters.stageSnapshot(stageNumber);

      // TODO(gianm): Should switch to *target* number of workers; this is *actual* number (may be less)
      final int workerCount = Ints.checkedCast(
          countersForStage.getWorkerCounters()
                          .stream()
                          .map(TalariaCountersSnapshot.WorkerCounters::getWorkerNumber)
                          .distinct()
                          .count()
      );

      // TODO(gianm): Should switch to *target* number of partitions; this is *actual* number (may be less)
      final int partitionCount = Ints.checkedCast(
          countersForStage.getWorkerCounters()
                          .stream()
                          .map(TalariaCountersSnapshot.WorkerCounters::getCountersMap)
                          .flatMap(countersMap -> countersMap.values().stream())
                          .flatMap(Collection::stream)
                          .mapToInt(TalariaCountersSnapshot.ChannelCounters::getPartitionNumber)
                          .filter(partitionNumber -> partitionNumber >= 0)
                          .distinct()
                          .count()
      );

      final Interval stageRuntimeInterval = stageRuntimeMap.get(stageNumber);
      final DateTime stageStartTime = stageRuntimeInterval == null ? null : stageRuntimeInterval.getStart();
      final long stageDuration = stageRuntimeInterval == null ? 0 : stageRuntimeInterval.toDurationMillis();

      final Stage stage = new Stage(
          stageNumber,
          stageDef.getInputStageIds().stream().map(StageId::getStageNumber).collect(Collectors.toList()),
          stageDef.getProcessorFactory().getClass().getSimpleName(),
          stagePhaseMap.get(stageNumber),
          workerCount,
          partitionCount,
          stageStartTime,
          stageDuration,
          stageDef.getShuffleSpec().map(ShuffleSpec::getClusterBy).orElse(null),
          countersForStage,
          stageQueryMap.get(stageNumber)
      );
      stages.add(stage);
    }

    return new TalariaStagesTaskReportPayload(stages);
  }

  @JsonProperty
  public List<Stage> getStages()
  {
    return stages;
  }

  public static class Stage
  {
    private final int stageNumber;
    private final List<Integer> inputStages;
    private final String stageType;
    private final int workerCount;
    private final int partitionCount;
    private final DateTime startTime;
    private final long duration;

    @Nullable
    private final ControllerStagePhase phase;

    @Nullable
    private final ClusterBy clusterBy;

    @Nullable
    private final TalariaCountersSnapshot workerCounters;

    // Note: the query is properly a QueryKit concept, not a core Talaria concept. But it's here so we can make
    // the reports more informative. It can be null if the Talaria query did not come from QueryKit, and that's fine.
    @Nullable
    private final Query<?> query;

    @JsonCreator
    private Stage(
        @JsonProperty("stageNumber") final int stageNumber,
        @JsonProperty("inputStages") final List<Integer> inputStages,
        @JsonProperty("stageType") final String stageType,
        @JsonProperty("phase") @Nullable final ControllerStagePhase phase,
        @JsonProperty("workerCount") final int workerCount,
        @JsonProperty("partitionCount") final int partitionCount,
        @JsonProperty("startTime") @Nullable final DateTime startTime,
        @JsonProperty("duration") final long duration,
        @JsonProperty("clusterBy") @Nullable final ClusterBy clusterBy,
        @JsonProperty("workerCounters") @Nullable final TalariaCountersSnapshot workerCounters,
        @JsonProperty("query") @Nullable final Query<?> query
    )
    {
      this.stageNumber = stageNumber;
      this.inputStages = Preconditions.checkNotNull(inputStages, "inputStages");
      this.stageType = Preconditions.checkNotNull(stageType, "stageType");
      this.workerCount = workerCount;
      this.partitionCount = partitionCount;
      this.startTime = startTime;
      this.duration = duration;
      this.phase = phase;
      this.clusterBy = clusterBy;
      this.workerCounters = workerCounters;
      this.query = query;
    }

    @JsonProperty
    public int getStageNumber()
    {
      return stageNumber;
    }

    @JsonProperty
    public List<Integer> getInputStages()
    {
      return inputStages;
    }

    @JsonProperty
    public String getStageType()
    {
      return stageType;
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

    @Nullable
    @JsonProperty("clusterBy")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ClusterBy getClusterBy()
    {
      return clusterBy;
    }

    @Nullable
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public TalariaCountersSnapshot getWorkerCounters()
    {
      return workerCounters;
    }

    @Nullable
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Query<?> getQuery()
    {
      return query;
    }
  }
}
