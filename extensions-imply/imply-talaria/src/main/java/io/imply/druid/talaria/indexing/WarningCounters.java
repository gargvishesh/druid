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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WarningCounters
{
  private final ConcurrentHashMap<String, Long> warningCodeCounter = new ConcurrentHashMap<>();

  public void incrementWarningCount(String errorCode)
  {
    warningCodeCounter.compute(errorCode, (ignored, oldCount) -> oldCount == null ? 1 : oldCount + 1);
  }

  public long totalWarningCount()
  {
    return warningCodeCounter.values().stream().reduce(0L, Long::sum);
  }

  public WarningCountersSnapshot snapshot()
  {
    Map<String, Long> countCopy = ImmutableMap.copyOf(warningCodeCounter);
    return new WarningCountersSnapshot(countCopy);
  }

  public static class WarningCountersSnapshot
  {
    private final Map<String, Long> warningCount;

    @JsonCreator
    public WarningCountersSnapshot(
        @JsonProperty("warningCount") Map<String, Long> warningCount
    )
    {
      this.warningCount = Preconditions.checkNotNull(warningCount, "warningCount");
    }

    @JsonProperty("warningCount")
    public Map<String, Long> getWarningCount()
    {
      return warningCount;
    }
  }
}
