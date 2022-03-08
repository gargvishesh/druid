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

  public void addAll(final TalariaCountersSnapshot snapshot)
  {
    for (final TalariaCountersSnapshot.WorkerCounters workerSnapshot : snapshot.getWorkerCounters()) {
      for (Map.Entry<TalariaCounterType, List<TalariaCountersSnapshot.ChannelCounters>> entry :
          workerSnapshot.getCountersMap().entrySet()) {
        final TalariaCounterType counterType = entry.getKey();
        final List<TalariaCountersSnapshot.ChannelCounters> channelSnapshots = entry.getValue();

        for (TalariaCountersSnapshot.ChannelCounters channelSnapshot : channelSnapshots) {
          getOrCreateChannelCounters(
              counterType,
              workerSnapshot.getWorkerNumber(),
              channelSnapshot.getStageNumber(),
              channelSnapshot.getPartitionNumber()
          ).addCounters(
              channelSnapshot.getFrames(),
              channelSnapshot.getRows(),
              channelSnapshot.getBytes(),
              channelSnapshot.getFiles()
          );
        }
        for (TalariaCountersSnapshot.SortProgressTracker sortProgressTrackerEntry : workerSnapshot.getSortProgress()) {
          // Worker sends one (updated) value of the snapshot per stage to the leader. Therefore, we only need to add or
          // replace (if present) the present value with the new value for that stage number
          workerCountersMap.get(workerSnapshot.getWorkerNumber()).sortProgressTrackerMap.compute(
              sortProgressTrackerEntry.getStageNumber(),
              (ignored1, ignored2) -> {
                SuperSorterProgressSnapshot superSorterProgressSnapshot = sortProgressTrackerEntry.getSortProgress();
                return SuperSorterProgressTracker.createWithSnapshot(superSorterProgressSnapshot);
              }
          );
        }
      }
    }
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

      workerCountersSnapshot.add(
          new TalariaCountersSnapshot.WorkerCounters(
              workerEntry.getKey(),
              workerSnapshotMap,
              sortProgressTrackerSnapshotList
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

        final long inputRows = workerCounters.getTotalRows(TalariaCounterType.INPUT, stageNumber);
        final long preShuffleRows = workerCounters.getTotalRows(TalariaCounterType.PRESHUFFLE, stageNumber);
        final long outputRows = workerCounters.getTotalRows(TalariaCounterType.OUTPUT, stageNumber);

        sb.append(inputRows);

        if (preShuffleRows > 0) {
          sb.append("~").append(preShuffleRows);
        }

        sb.append(">").append(outputRows);
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

    public Map<Integer, SuperSorterProgressTracker> getSortProgressTrackersMap()
    {
      return sortProgressTrackerMap;
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
