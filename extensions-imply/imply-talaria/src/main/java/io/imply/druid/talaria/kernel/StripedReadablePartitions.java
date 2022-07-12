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
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class StripedReadablePartitions implements ReadablePartitions
{
  private final int stageNumber;
  private final int numWorkers;
  private final IntSortedSet partitionNumbers;

  StripedReadablePartitions(final int stageNumber, final int numWorkers, final IntSortedSet partitionNumbers)
  {
    this.stageNumber = stageNumber;
    this.numWorkers = numWorkers;
    this.partitionNumbers = partitionNumbers;
  }

  @JsonCreator
  private StripedReadablePartitions(
      @JsonProperty("stageNumber") final int stageNumber,
      @JsonProperty("numWorkers") final int numWorkers,
      @JsonProperty("partitionNumbers") final Set<Integer> partitionNumbers
  )
  {
    this(stageNumber, numWorkers, new IntAVLTreeSet(partitionNumbers));
  }

  @Override
  public Iterator<ReadablePartition> iterator()
  {
    return Iterators.transform(
        partitionNumbers.iterator(),
        partitionNumber -> ReadablePartition.striped(stageNumber, numWorkers, partitionNumber)
    );
  }

  @Override
  public List<ReadablePartitions> split(final int maxNumSplits)
  {
    final List<ReadablePartitions> retVal = new ArrayList<>();

    for (List<Integer> entries : SplitUtils.makeSplits(partitionNumbers.iterator(), maxNumSplits)) {
      if (!entries.isEmpty()) {
        retVal.add(new StripedReadablePartitions(stageNumber, numWorkers, new IntAVLTreeSet(entries)));
      }
    }

    return retVal;
  }

  @JsonProperty
  int getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty
  int getNumWorkers()
  {
    return numWorkers;
  }

  @JsonProperty
  IntSortedSet getPartitionNumbers()
  {
    return partitionNumbers;
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
    StripedReadablePartitions that = (StripedReadablePartitions) o;
    return numWorkers == that.numWorkers && Objects.equals(partitionNumbers, that.partitionNumbers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(numWorkers, partitionNumbers);
  }

  @Override
  public String toString()
  {
    return "StripedReadablePartitions{" +
           "stageNumber=" + stageNumber +
           ", numWorkers=" + numWorkers +
           ", partitionNumbers=" + partitionNumbers +
           '}';
  }
}
