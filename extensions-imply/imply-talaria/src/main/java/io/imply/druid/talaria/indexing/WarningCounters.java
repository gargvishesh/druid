/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WarningCounters
{
  private final ConcurrentHashMap<String, Long> count = new ConcurrentHashMap<>();

  public void incrementErrorCount(String errorCode)
  {
    count.compute(errorCode, (ignored, oldCount) -> oldCount == null ? 1 : oldCount + 1);
  }

  public long totalErrorCount(String errorCode)
  {
    return count.getOrDefault(errorCode, 0L);
  }

  public synchronized long totalErrorCount()
  {
    return count.values().stream().reduce(0L, Long::sum);
  }

  public WarningCountersSnapshot snapshot()
  {
    Map<String, Long> countCopy = new HashMap<>(count);
    return new WarningCountersSnapshot(countCopy);
  }
}
