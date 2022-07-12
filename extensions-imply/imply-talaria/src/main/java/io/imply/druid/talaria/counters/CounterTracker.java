/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.counters;

import io.imply.druid.talaria.indexing.SuperSorterProgressTracker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Class that tracks query counters for a particular worker in a particular stage.
 *
 * Counters are all tracked on a (stage, worker, counter) basis by the {@link #countersMap} object.
 *
 * Immutable {@link CounterSnapshots} snapshots can be created by {@link #snapshot()}.
 */
public class CounterTracker
{
  private final ConcurrentHashMap<String, QueryCounter> countersMap = new ConcurrentHashMap<>();

  public ChannelCounters channel(final String name)
  {
    return counter(name, ChannelCounters::new);
  }

  public SuperSorterProgressTracker sortProgress()
  {
    return counter(CounterNames.sortProgress(), SuperSorterProgressTracker::new);
  }

  public WarningCounters warnings()
  {
    return counter(CounterNames.warnings(), WarningCounters::new);
  }

  @SuppressWarnings("unchecked")
  public <T extends QueryCounter> T counter(final String counterName, final Supplier<T> newCounterFn)
  {
    return (T) countersMap.computeIfAbsent(counterName, ignored -> newCounterFn.get());
  }

  public CounterSnapshots snapshot()
  {
    final Map<String, QueryCounterSnapshot> m = new HashMap<>();

    for (final Map.Entry<String, QueryCounter> entry : countersMap.entrySet()) {
      final QueryCounterSnapshot counterSnapshot = entry.getValue().snapshot();
      if (counterSnapshot != null) {
        m.put(entry.getKey(), counterSnapshot);
      }
    }

    return new CounterSnapshots(m);
  }
}
