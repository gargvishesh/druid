/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * TODO(gianm): Javadoc including the fact that partitions must be iterated *in order*
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "collected", value = CollectedReadablePartitions.class),
    @JsonSubTypes.Type(name = "striped", value = StripedReadablePartitions.class),
    @JsonSubTypes.Type(name = "combined", value = CombinedReadablePartitions.class)
})
public interface ReadablePartitions extends Iterable<ReadablePartition>
{
  List<ReadablePartitions> split(int maxNumSplits);

  static ReadablePartitions empty()
  {
    return new CombinedReadablePartitions(Collections.emptyList());
  }

  static ReadablePartitions combine(List<ReadablePartitions> readablePartitions)
  {
    return new CombinedReadablePartitions(readablePartitions);
  }

  static StripedReadablePartitions striped(
      final int stageNumber,
      final int numWorkers,
      final int numPartitions
  )
  {
    final IntAVLTreeSet partitionNumbers = new IntAVLTreeSet();
    for (int i = 0; i < numPartitions; i++) {
      partitionNumbers.add(i);
    }

    return new StripedReadablePartitions(stageNumber, numWorkers, partitionNumbers);
  }

  static CollectedReadablePartitions collected(
      final int stageNumber,
      final Map<Integer, Integer> partitionToWorkerMap
  )
  {
    if (partitionToWorkerMap instanceof Int2IntSortedMap) {
      return new CollectedReadablePartitions(stageNumber, (Int2IntSortedMap) partitionToWorkerMap);
    } else {
      return new CollectedReadablePartitions(stageNumber, new Int2IntAVLTreeMap(partitionToWorkerMap));
    }
  }

}

