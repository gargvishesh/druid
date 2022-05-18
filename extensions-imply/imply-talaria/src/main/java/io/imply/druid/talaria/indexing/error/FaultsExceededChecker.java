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
import io.imply.druid.talaria.indexing.TalariaCounters;
import io.imply.druid.talaria.indexing.TalariaCountersSnapshot;
import io.imply.druid.talaria.indexing.WarningCounters;
import org.apache.druid.java.util.common.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps a track of the warnings that have been so far and returns if any type has exceeded their designated limit
 * This class is thread safe
 */
public class FaultsExceededChecker
{
  final Map<String, Long> maxFaultsAllowedCount;
  final ConcurrentHashMap<String, Long> currentFaultsEncounteredCount = new ConcurrentHashMap<>();

  public FaultsExceededChecker(final Map<String, Long> maxFaultsAllowedCount)
  {
    maxFaultsAllowedCount.forEach((ignored, count) ->
                                      Preconditions.checkArgument(count >= 0 || count == -1, "invalid count")
    );
    this.maxFaultsAllowedCount = maxFaultsAllowedCount;
  }

  public Optional<Pair<String, Long>> addFaultsAndCheckIfExceeded(final List<String> talariaFaults)
  {
    Optional<Pair<String, Long>> ret = Optional.empty();
    for (final String talariaFault : talariaFaults) {
      Long limit = maxFaultsAllowedCount.getOrDefault(talariaFault, -1L);
      currentFaultsEncounteredCount.compute(
          talariaFault,
          (ignored, currentCount) -> currentCount == null ? 1L : currentCount + 1L
      );
      if (limit != -1) {
        if (currentFaultsEncounteredCount.get(talariaFault) > limit) {
          ret = Optional.of(Pair.of(talariaFault, limit));
        }
      }
    }
    return ret;
  }

  public Optional<Pair<String, Long>> addFaultsAndCheckIfExceeded2(final TalariaCounters talariaCounters)
  {
    Optional<Pair<String, Long>> ret = Optional.empty();
    Map<String, Long> workerWarnings = talariaCounters.getErrorCodeToTotalErrorsMap();
    workerWarnings.entrySet().stream().anyMatch(entry -> {
      long limit = maxFaultsAllowedCount.getOrDefault(entry.getKey(), -1L);
      boolean passed = limit == -1 || entry.getValue() <= limit;
      return !passed;
    });
    return ret;
  }

  public Optional<Pair<String, Long>> addFaultsAndCheckIfExceeded3(final Map<String, TalariaCountersSnapshot.WorkerCounters> taskCounters)
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
