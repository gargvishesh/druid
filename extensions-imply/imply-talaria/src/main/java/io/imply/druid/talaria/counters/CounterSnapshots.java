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
import com.google.common.collect.ImmutableSortedMap;

import java.util.Map;
import java.util.Objects;

/**
 * Named counter snapshots. Immutable. Often part of a {@link CounterSnapshotsTree}.
 */
public class CounterSnapshots
{
  private final Map<String, QueryCounterSnapshot> snapshotMap;

  @JsonCreator
  public CounterSnapshots(final Map<String, QueryCounterSnapshot> snapshotMap)
  {
    this.snapshotMap = ImmutableSortedMap.copyOf(snapshotMap, CounterNames.comparator());
  }

  public Map<String, QueryCounterSnapshot> getMap()
  {
    return snapshotMap;
  }

  public boolean isEmpty()
  {
    return snapshotMap.isEmpty();
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
    CounterSnapshots that = (CounterSnapshots) o;
    return Objects.equals(snapshotMap, that.snapshotMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(snapshotMap);
  }

  @Override
  public String toString()
  {
    return snapshotMap.toString();
  }
}
