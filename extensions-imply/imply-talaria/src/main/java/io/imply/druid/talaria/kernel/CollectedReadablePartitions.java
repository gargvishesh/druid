/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CollectedReadablePartitions implements ReadablePartitions
{
  private final int stageNumber;
  private final Int2IntSortedMap partitionToWorkerMap;

  CollectedReadablePartitions(final int stageNumber, final Int2IntSortedMap partitionToWorkerMap)
  {
    this.stageNumber = stageNumber;
    this.partitionToWorkerMap = partitionToWorkerMap;
  }

  @JsonCreator
  private CollectedReadablePartitions(
      @JsonProperty("stageNumber") final int stageNumber,
      @JsonProperty("partitionToWorkerMap") final Map<Integer, Integer> partitionToWorkerMap
  )
  {
    this(stageNumber, new Int2IntAVLTreeMap(partitionToWorkerMap));
  }

  @Override
  public Iterator<ReadablePartition> iterator()
  {
    return Iterators.transform(
        partitionToWorkerMap.int2IntEntrySet().iterator(),
        entry -> ReadablePartition.collected(stageNumber, entry.getIntValue(), entry.getIntKey())
    );
  }

  @Override
  public List<ReadablePartitions> split(int maxNumSplits)
  {
    return SplitUtils.makeSplits(partitionToWorkerMap.int2IntEntrySet().iterator(), maxNumSplits)
                     .stream()
                     .map(
                         entries -> {
                           final Int2IntSortedMap map = new Int2IntAVLTreeMap();

                           for (final Int2IntMap.Entry entry : entries) {
                             map.put(entry.getIntKey(), entry.getIntValue());
                           }

                           return new CollectedReadablePartitions(stageNumber, map);
                         }
                     )
                     .collect(Collectors.toList());
  }

  @JsonProperty
  int getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty
  Int2IntSortedMap getPartitionToWorkerMap()
  {
    return partitionToWorkerMap;
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
    CollectedReadablePartitions that = (CollectedReadablePartitions) o;
    return stageNumber == that.stageNumber && Objects.equals(partitionToWorkerMap, that.partitionToWorkerMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageNumber, partitionToWorkerMap);
  }

  @Override
  public String toString()
  {
    return "CollectedReadablePartitions{" +
           "stageNumber=" + stageNumber +
           ", partitionToWorkerMap=" + partitionToWorkerMap +
           '}';
  }
}
