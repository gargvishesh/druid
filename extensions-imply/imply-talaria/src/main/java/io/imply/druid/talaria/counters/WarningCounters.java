/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.counters;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Counters for warnings. Created by {@link CounterTracker#warnings()}.
 */
@JsonTypeName("warnings")
public class WarningCounters implements QueryCounter
{
  private final ConcurrentHashMap<String, Long> warningCodeCounter = new ConcurrentHashMap<>();

  public void incrementWarningCount(String errorCode)
  {
    warningCodeCounter.compute(errorCode, (ignored, oldCount) -> oldCount == null ? 1 : oldCount + 1);
  }

  @Override
  @Nullable
  public Snapshot snapshot()
  {
    if (warningCodeCounter.isEmpty()) {
      return null;
    }

    final Map<String, Long> countCopy = ImmutableMap.copyOf(warningCodeCounter);
    return new Snapshot(countCopy);
  }

  @JsonTypeName("warnings")
  public static class Snapshot implements QueryCounterSnapshot
  {
    private final Map<String, Long> warningCountMap;

    @JsonCreator
    public Snapshot(Map<String, Long> warningCountMap)
    {
      this.warningCountMap = Preconditions.checkNotNull(warningCountMap, "warningCountMap");
    }

    @JsonValue
    public Map<String, Long> getWarningCountMap()
    {
      return warningCountMap;
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
      Snapshot snapshot = (Snapshot) o;
      return Objects.equals(warningCountMap, snapshot.warningCountMap);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(warningCountMap);
    }
  }
}
