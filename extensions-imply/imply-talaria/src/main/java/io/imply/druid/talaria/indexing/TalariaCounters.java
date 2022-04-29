/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.kernel.StagePartition;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class that tracks query counters.
 * <p>
 * Counters are all tracked on a per-worker basis by the {@link #workerCountersMap} object.
 * <p>
 * Immutable {@link TalariaCountersSnapshot} snapshots can be created by {@link #snapshot()}. These are used for
 * worker-to-controller counters propagation (see {@link TalariaIndexerTaskClient#postCounters}) and reporting
 * to end users (see {@link io.imply.druid.talaria.indexing.report.TalariaTaskReportPayload#getCounters}).
 */
public class TalariaCounters
{
  // Worker number -> worker counters
  private final ConcurrentHashMap<Integer, WorkerCounters> workerCountersMap = new ConcurrentHashMap<>();

  @Nullable
  public SuperSorterProgressTracker getOrCreateSortProgressTracker(
      final int workerNumber,
      final int stageNumber
  )
  {
    return workerCounters(workerNumber).getOrCreateSortProgressTracker(stageNumber);
  }

  public ChannelCounters getOrCreateChannelCounters(
      final TalariaCounterType counterType,
      final int workerNumber,
      final int stageNumber,
      final int partitionNumber
  )
  {
    return workerCounters(workerNumber)
        .countersMap
        .computeIfAbsent(counterType, ignored -> new ConcurrentHashMap<>())
        .computeIfAbsent(new StagePartitionNumber(stageNumber, partitionNumber), ignored -> new ChannelCounters());
  }

  public WarningCounters getOrCreateWarningCounters(
      final int workerNumber,
      final int stageNumber
  )
  {
    return workerCounters(workerNumber).getOrCreateWarningCounters(stageNumber);
  }

  public TalariaCountersSnapshot snapshot()
  {
    final List<TalariaCountersSnapshot.WorkerCounters> workerCountersSnapshot = new ArrayList<>();

    for (final Map.Entry<Integer, WorkerCounters> workerEntry : workerCountersMap.entrySet()) {
      final EnumMap<TalariaCounterType, List<TalariaCountersSnapshot.ChannelCounters>> workerSnapshotMap =
          new EnumMap<>(TalariaCounterType.class);

      for (Map.Entry<TalariaCounterType, ConcurrentHashMap<StagePartitionNumber, ChannelCounters>> counterTypeEntry :
          workerEntry.getValue().countersMap.entrySet()) {
        final TalariaCounterType counterType = counterTypeEntry.getKey();
        final ConcurrentHashMap<StagePartitionNumber, ChannelCounters> channelCountersMap = counterTypeEntry.getValue();

        for (Map.Entry<StagePartitionNumber, ChannelCounters> channelEntry : channelCountersMap.entrySet()) {
          final StagePartitionNumber stagePartitionNumber = channelEntry.getKey();
          final ChannelCounters channelCounters = channelEntry.getValue();

          workerSnapshotMap.computeIfAbsent(counterType, ignored -> new ArrayList<>()).add(
              new TalariaCountersSnapshot.ChannelCounters(
                  stagePartitionNumber.getStageNumber(),
                  stagePartitionNumber.getPartitionNumber(),
                  channelCounters.frames.get(),
                  channelCounters.rows.get(),
                  channelCounters.bytes.get(),
                  channelCounters.files.get()
              )
          );
        }
      }

      // Sort by stageNumber + partitionNumber so the snapshots look nice.
      for (final List<TalariaCountersSnapshot.ChannelCounters> countersList : workerSnapshotMap.values()) {
        countersList.sort(
            Comparator.comparing(
                channelCounters -> new StagePartitionNumber(
                    channelCounters.getStageNumber(),
                    channelCounters.getPartitionNumber()
                )
            )
        );
      }

      List<TalariaCountersSnapshot.SortProgressTracker> sortProgressTrackerSnapshotList = new ArrayList<>();
      for (Map.Entry<Integer, SuperSorterProgressTracker> sortProgressTrackerEntry : workerEntry.getValue().sortProgressTrackerMap.entrySet()) {
        sortProgressTrackerSnapshotList.add(
            new TalariaCountersSnapshot.SortProgressTracker(
                sortProgressTrackerEntry.getKey(), sortProgressTrackerEntry.getValue().snapshot()
            )
        );
      }

      List<TalariaCountersSnapshot.WarningCounters> warningCountersList = new ArrayList<>();
      for (Map.Entry<Integer, WarningCounters> warningCountersEntry : workerEntry.getValue().warningCountersMap.entrySet()) {
        warningCountersList.add(
            new TalariaCountersSnapshot.WarningCounters(
                warningCountersEntry.getKey(), warningCountersEntry.getValue().snapshot()
            )
        );
      }

      workerCountersSnapshot.add(
          new TalariaCountersSnapshot.WorkerCounters(
              workerEntry.getKey(),
              workerSnapshotMap,
              sortProgressTrackerSnapshotList,
              warningCountersList
          )
      );
    }

    return new TalariaCountersSnapshot(workerCountersSnapshot);
  }

  private WorkerCounters workerCounters(final int workerNumber)
  {
    return workerCountersMap.computeIfAbsent(workerNumber, ignored -> new WorkerCounters());
  }

  public String stateString()
  {
    final StringBuilder sb = new StringBuilder();

    for (Map.Entry<Integer, WorkerCounters> workerEntry : workerCountersMap.entrySet()) {
      final WorkerCounters workerCounters = workerEntry.getValue();
      final IntSortedSet allStageNumbers = new IntRBTreeSet();

      for (ConcurrentHashMap<StagePartitionNumber, ChannelCounters> countersMap : workerCounters.countersMap.values()) {
        for (final StagePartitionNumber stagePartitionNumber : countersMap.keySet()) {
          allStageNumbers.add(stagePartitionNumber.getStageNumber());
        }
      }

      boolean first = true;

      for (final int stageNumber : allStageNumbers) {
        if (first) {
          first = false;
        } else {
          sb.append("; ");
        }

        sb.append(StringUtils.format("S%d:W%d:", stageNumber, workerEntry.getKey()));

        final long inputRows =
            workerCounters.getTotalRows(TalariaCounterType.INPUT_STAGE, stageNumber)
            + workerCounters.getTotalRows(TalariaCounterType.INPUT_EXTERNAL, stageNumber)
            + workerCounters.getTotalRows(TalariaCounterType.INPUT_DRUID, stageNumber);
        final long processorRows = workerCounters.getTotalRows(TalariaCounterType.PROCESSOR, stageNumber);
        final long sortRows = workerCounters.getTotalRows(TalariaCounterType.SORT, stageNumber);
        final long workerWarnings = workerCounters.getOrCreateWarningCounters(stageNumber).totalWarningCount();

        sb.append(inputRows);

        if (processorRows > 0) {
          sb.append(" Processed Rows-").append(processorRows);

          if (sortRows > 0) {
            sb.append(" Sorted Rows-").append(sortRows);
          }
        }

        if (workerWarnings > 0) {
          sb.append(" Worker Warnings-").append(workerWarnings);
        }
      }
    }

    return sb.toString();
  }

  private static class WorkerCounters
  {
    // TODO(gianm): more generic way of handling these counters: get rid of the copy/paste code
    private final ConcurrentHashMap<TalariaCounterType, ConcurrentHashMap<StagePartitionNumber, ChannelCounters>> countersMap =
        new ConcurrentHashMap<>();

    // stage number -> sort progress
    private final ConcurrentHashMap<Integer, SuperSorterProgressTracker> sortProgressTrackerMap = new ConcurrentHashMap<>();

    // stage number -> warnings
    private final ConcurrentHashMap<Integer, WarningCounters> warningCountersMap = new ConcurrentHashMap<>();

    private WorkerCounters()
    {
      // Only created in this file.
    }

    private long getTotalRows(final TalariaCounterType counterType, final int stageNumber)
    {
      final ConcurrentHashMap<StagePartitionNumber, ChannelCounters> channelCountersMap = countersMap.get(counterType);

      if (channelCountersMap == null) {
        return 0;
      }

      long total = 0;

      for (final Map.Entry<StagePartitionNumber, ChannelCounters> entry : channelCountersMap.entrySet()) {
        if (entry.getKey().getStageNumber() == stageNumber) {
          total += entry.getValue().rows.get();
        }
      }

      return total;
    }

    public SuperSorterProgressTracker getOrCreateSortProgressTracker(int stageNumber)
    {
      return sortProgressTrackerMap.computeIfAbsent(stageNumber, ignored -> new SuperSorterProgressTracker());
    }

    public WarningCounters getOrCreateWarningCounters(int stageNumber)
    {
      return warningCountersMap.computeIfAbsent(stageNumber, ignored -> new WarningCounters());
    }
  }

  /**
   * Like {@link StagePartition}, but only contains the stage number, not the stage ID.
   */
  private static class StagePartitionNumber implements Comparable<StagePartitionNumber>
  {
    private final int stageNumber;
    private final int partitionNumber;

    public StagePartitionNumber(int stageNumber, int partitionNumber)
    {
      this.stageNumber = stageNumber;
      this.partitionNumber = partitionNumber;
    }

    public int getPartitionNumber()
    {
      return partitionNumber;
    }

    public int getStageNumber()
    {
      return stageNumber;
    }

    @Override
    public int compareTo(final StagePartitionNumber other)
    {
      final int cmp = Integer.compare(stageNumber, other.stageNumber);
      if (cmp != 0) {
        return cmp;
      } else {
        return Integer.compare(partitionNumber, other.partitionNumber);
      }
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
      StagePartitionNumber that = (StagePartitionNumber) o;
      return stageNumber == that.stageNumber && partitionNumber == that.partitionNumber;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(stageNumber, partitionNumber);
    }

    @Override
    public String toString()
    {
      return "StagePartitionNumber{" +
             "stageNumber=" + stageNumber +
             ", partitionNumber=" + partitionNumber +
             '}';
    }
  }

  public static class ChannelCounters
  {
    private final AtomicLong frames = new AtomicLong();
    private final AtomicLong rows = new AtomicLong();
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicLong files = new AtomicLong();

    private ChannelCounters()
    {
      // Only created in this file.
    }

    public void addFrame(final Frame frame)
    {
      frames.incrementAndGet();
      rows.addAndGet(frame.numRows());
      bytes.addAndGet(frame.numBytes());
    }

    public void addCounters(final long frames, final long rows, final long bytes, final long files)
    {
      this.frames.addAndGet(frames);
      this.rows.addAndGet(rows);
      this.bytes.addAndGet(bytes);
      this.files.addAndGet(files);
    }

    public void incrementRowCount()
    {
      this.rows.incrementAndGet();
    }

    public void incrementFileCount()
    {
      this.files.incrementAndGet();
    }
  }
}
