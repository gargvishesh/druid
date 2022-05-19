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
import io.imply.druid.talaria.indexing.TalariaCountersSnapshot;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;

import java.util.HashMap;
import java.util.List;
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
                StringUtils.format("Invalid limit for warning of type %s", warning)
            )
    );
    this.maxFaultsAllowedCount = maxFaultsAllowedCount;
  }

  /**
   * @param taskCounters WorkerCounters have the count of the warnings generated per worker
   * @return An optional which is empty if the faults count in the present in the task counters donot exceed their
   * prescribed limit, else it contains the errorCode and the maximum allowed faults for that errorCode
   */
  public Optional<Pair<String, Long>> addFaultsAndCheckIfExceeded(final Map<String, TalariaCountersSnapshot.WorkerCounters> taskCounters)
  {
    Map<String, Long> allWarnings = new HashMap<>();
    for (Map.Entry<String, TalariaCountersSnapshot.WorkerCounters> workerCountersEntry : taskCounters.entrySet()) {
      List<TalariaCountersSnapshot.WarningCounters> warningCountersList = workerCountersEntry.getValue()
                                                                                             .getWarningCounters();
      for (TalariaCountersSnapshot.WarningCounters warningCounters : warningCountersList) {
        Map<String, Long> warningCount = warningCounters.getWarningCountersSnapshot().getWarningCount();
        for (Map.Entry<String, Long> entry : warningCount.entrySet()) {
          allWarnings.compute(
              entry.getKey(),
              (ignored, value) -> value == null ? entry.getValue() : value + entry.getValue()
          );
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
