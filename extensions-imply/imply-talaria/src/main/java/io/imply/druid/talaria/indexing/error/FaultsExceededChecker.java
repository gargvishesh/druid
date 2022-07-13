/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.counters.CounterNames;
import io.imply.druid.talaria.counters.CounterSnapshots;
import io.imply.druid.talaria.counters.CounterSnapshotsTree;
import io.imply.druid.talaria.counters.WarningCounters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Keeps a track of the warnings that have been so far and returns if any type has exceeded their designated limit
 * This class is thread safe
 */
public class FaultsExceededChecker
{
  final Map<String, Long> maxFaultsAllowedCount;

  public FaultsExceededChecker(final Map<String, Long> maxFaultsAllowedCount)
  {
    maxFaultsAllowedCount.forEach(
        (warning, count) ->
            Preconditions.checkArgument(
                count >= 0 || count == -1,
                StringUtils.format("Invalid limit of %d supplied for warnings of type %s. "
                                   + "Limit can be greater than or equal to -1.", count, warning)
            )
    );
    this.maxFaultsAllowedCount = maxFaultsAllowedCount;
  }

  /**
   * @param snapshotsTree WorkerCounters have the count of the warnings generated per worker
   *
   * @return An optional which is empty if the faults count in the present in the task counters donot exceed their
   * prescribed limit, else it contains the errorCode and the maximum allowed faults for that errorCode
   */
  public Optional<Pair<String, Long>> addFaultsAndCheckIfExceeded(CounterSnapshotsTree snapshotsTree)
  {
    final Map<Integer, Map<Integer, CounterSnapshots>> snapshotsMap = snapshotsTree.copyMap();

    Map<String, Long> allWarnings = new HashMap<>();
    for (Map.Entry<Integer, Map<Integer, CounterSnapshots>> stageEntry : snapshotsMap.entrySet()) {
      for (Map.Entry<Integer, CounterSnapshots> workerEntry : stageEntry.getValue().entrySet()) {
        final WarningCounters.Snapshot warningsSnapshot =
            (WarningCounters.Snapshot) workerEntry.getValue().getMap().get(CounterNames.warnings());

        if (warningsSnapshot != null) {
          for (Map.Entry<String, Long> entry : warningsSnapshot.getWarningCountMap().entrySet()) {
            allWarnings.compute(
                entry.getKey(),
                (ignored, value) -> value == null ? entry.getValue() : value + entry.getValue()
            );
          }
        }
      }
    }

    for (Map.Entry<String, Long> totalWarningCountEntry : allWarnings.entrySet()) {
      long limit = maxFaultsAllowedCount.getOrDefault(totalWarningCountEntry.getKey(), -1L);
      boolean passed = limit == -1 || totalWarningCountEntry.getValue() <= limit;
      if (!passed) {
        return Optional.of(Pair.of(totalWarningCountEntry.getKey(), limit));
      }
    }
    return Optional.empty();
  }

}
