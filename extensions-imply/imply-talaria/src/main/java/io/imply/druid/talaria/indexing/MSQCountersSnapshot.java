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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.indexing.WarningCounters.WarningCountersSnapshot;

import javax.annotation.Nullable;
import java.util.EnumMap;
import java.util.List;

/**
 * Like {@link TalariaCounters}, but immutable.
 */
public class MSQCountersSnapshot
{
  private final List<WorkerCounters> workerCounters;

  @JsonCreator
  @VisibleForTesting
  public MSQCountersSnapshot(final List<WorkerCounters> workerCounters)
  {
    this.workerCounters = Preconditions.checkNotNull(workerCounters, "workerCounters");
  }

  @JsonValue
  public List<WorkerCounters> getWorkerCounters()
  {
    return workerCounters;
  }

  public static class WorkerCounters
  {
    private final int workerNumber;
    private final EnumMap<MSQCounterType, List<ChannelCounters>> countersMap;

    private final List<SortProgressTracker> sortProgress;
    private final List<WarningCounters> warningCounters;

    @JsonCreator
    public WorkerCounters(
        @JsonProperty("workerNumber") Integer workerNumber,
        @JsonProperty("counters") EnumMap<MSQCounterType, List<ChannelCounters>> countersMap,
        @Nullable @JsonProperty("sortProgress") List<SortProgressTracker> sortProgress,
        @JsonProperty ("warningCounters") List<WarningCounters> warningCounters
    )
    {
      this.workerNumber = Preconditions.checkNotNull(workerNumber, "workerNumber");
      this.countersMap = Preconditions.checkNotNull(countersMap, "countersMap");
      this.sortProgress = Preconditions.checkNotNull(sortProgress, "sortProgress");
      this.warningCounters = Preconditions.checkNotNull(warningCounters, "warningCounters");
    }

    @JsonProperty
    public int getWorkerNumber()
    {
      return workerNumber;
    }

    @JsonProperty("counters")
    public EnumMap<MSQCounterType, List<ChannelCounters>> getCountersMap()
    {
      return countersMap;
    }

    @JsonProperty("sortProgress")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<SortProgressTracker> getSortProgress()
    {
      return sortProgress;
    }

    @JsonProperty("warningCounters")
    public List<WarningCounters> getWarningCounters()
    {
      return warningCounters;
    }
  }

  public static class WarningCounters
  {
    private final WarningCountersSnapshot warningCountersSnapshot;
    private final int stageNumber;

    @JsonCreator
    public WarningCounters(
        @JsonProperty("stageNumber") int stageNumber,
        @JsonProperty("warningCounters")
            WarningCountersSnapshot warningCountersSnapshot
    )
    {
      this.stageNumber = stageNumber;
      this.warningCountersSnapshot = warningCountersSnapshot;
    }

    @JsonProperty("warningCounters")
    public WarningCountersSnapshot getWarningCountersSnapshot()
    {
      return warningCountersSnapshot;
    }

    @JsonProperty("stageNumber")
    public int getStageNumber()
    {
      return stageNumber;
    }
  }

  public static class ChannelCounters
  {
    private final int stageNumber;
    private final int partitionNumber;
    private final long frames;
    private final long rows;
    private final long bytes;
    private final long files;

    @JsonCreator
    public ChannelCounters(
        @JsonProperty("stageNumber") int stageNumber,
        @JsonProperty("partitionNumber") int partitionNumber,
        @JsonProperty("frames") long frames,
        @JsonProperty("rows") long rows,
        @JsonProperty("bytes") long bytes,
        @JsonProperty("files") long files
    )
    {
      this.stageNumber = stageNumber;
      this.partitionNumber = partitionNumber;
      this.frames = frames;
      this.rows = rows;
      this.bytes = bytes;
      this.files = files;
    }

    @JsonProperty
    public int getStageNumber()
    {
      return stageNumber;
    }

    @JsonProperty
    public int getPartitionNumber()
    {
      return partitionNumber;
    }

    @JsonProperty
    public long getFrames()
    {
      return frames;
    }

    @JsonProperty
    public long getRows()
    {
      return rows;
    }

    @JsonProperty
    public long getBytes()
    {
      return bytes;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public long getFiles()
    {
      return files;
    }
  }

  public static class SortProgressTracker
  {
    private final int stageNumber;
    private final SuperSorterProgressSnapshot sortProgress;

    @JsonCreator
    public SortProgressTracker(
        @JsonProperty("stageNumber") int stageNumber,
        @JsonProperty("sortProgress") SuperSorterProgressSnapshot sortProgress
    )
    {
      this.stageNumber = stageNumber;
      this.sortProgress = sortProgress;
    }

    @JsonProperty
    public int getStageNumber()
    {
      return stageNumber;
    }

    @JsonProperty
    public SuperSorterProgressSnapshot getSortProgress()
    {
      return sortProgress;
    }
  }
}
