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
import org.apache.druid.java.util.common.Pair;

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
}
