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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Map;

/**
 * Tree of {@link CounterSnapshots} (named counter snapshots) organized by stage and worker.
 *
 * These are used for worker-to-controller counters propagation with
 * {@link io.imply.druid.talaria.exec.LeaderClient#postCounters} and reporting to end users with
 * {@link io.imply.druid.talaria.indexing.report.TalariaTaskReportPayload#getCounters}).
 *
 * The map is mutable, but tread-safe. The individual snapshot objects are immutable.
 */
public class CounterSnapshotsTree
{
  // stage -> worker -> counters
  @GuardedBy("snapshotsMap")
  private final Int2ObjectMap<Int2ObjectMap<CounterSnapshots>> snapshotsMap;

  public CounterSnapshotsTree()
  {
    this.snapshotsMap = new Int2ObjectAVLTreeMap<>();
  }

  @JsonCreator
  public static CounterSnapshotsTree fromMap(final Map<Integer, Map<Integer, CounterSnapshots>> map)
  {
    final CounterSnapshotsTree retVal = new CounterSnapshotsTree();
    retVal.putAll(map);
    return retVal;
  }

  public void put(final int stageNumber, final int workerNumber, final CounterSnapshots snapshots)
  {
    synchronized (snapshotsMap) {
      snapshotsMap.computeIfAbsent(stageNumber, ignored -> new Int2ObjectAVLTreeMap<>())
                  .put(workerNumber, snapshots);
    }
  }

  public void putAll(final CounterSnapshotsTree other)
  {
    putAll(other.copyMap());
  }

  public boolean isEmpty()
  {
    synchronized (snapshotsMap) {
      return snapshotsMap.isEmpty();
    }
  }

  @JsonValue
  public Map<Integer, Map<Integer, CounterSnapshots>> copyMap()
  {
    final Map<Integer, Map<Integer, CounterSnapshots>> retVal = new Int2ObjectAVLTreeMap<>();

    synchronized (snapshotsMap) {
      for (Int2ObjectMap.Entry<Int2ObjectMap<CounterSnapshots>> entry : snapshotsMap.int2ObjectEntrySet()) {
        retVal.put(entry.getIntKey(), new Int2ObjectAVLTreeMap<>(entry.getValue()));
      }
    }

    return retVal;
  }

  private void putAll(final Map<Integer, Map<Integer, CounterSnapshots>> otherMap)
  {
    synchronized (snapshotsMap) {
      for (Map.Entry<Integer, Map<Integer, CounterSnapshots>> stageEntry : otherMap.entrySet()) {
        for (Map.Entry<Integer, CounterSnapshots> workerEntry : stageEntry.getValue().entrySet()) {
          put(stageEntry.getKey(), workerEntry.getKey(), workerEntry.getValue());
        }
      }
    }
  }
}
