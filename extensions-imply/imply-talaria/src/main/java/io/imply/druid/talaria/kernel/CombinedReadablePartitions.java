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
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class CombinedReadablePartitions implements ReadablePartitions
{
  private final List<ReadablePartitions> readablePartitions;

  @JsonCreator
  public CombinedReadablePartitions(@JsonProperty("children") final List<ReadablePartitions> readablePartitions)
  {
    this.readablePartitions = readablePartitions;
  }

  @Override
  public List<ReadablePartitions> split(final int maxNumSplits)
  {
    // Split each item of "readablePartitions", then combine all the 0s, all the 1s, all the 2s, etc.
    final List<List<ReadablePartitions>> splits =
        readablePartitions.stream().map(rp -> rp.split(maxNumSplits)).collect(Collectors.toList());

    final List<ReadablePartitions> retVal = new ArrayList<>();

    for (int i = 0; i < maxNumSplits; i++) {
      final List<ReadablePartitions> combo = new ArrayList<>();

      for (int j = 0; j < readablePartitions.size(); j++) {
        if (splits.get(j).size() > i) {
          combo.add(splits.get(j).get(i));
        }
      }

      retVal.add(new CombinedReadablePartitions(combo));
    }

    return retVal;
  }

  @Override
  public Iterator<ReadablePartition> iterator()
  {
    return Iterables.concat(readablePartitions).iterator();
  }

  @JsonProperty("children")
  List<ReadablePartitions> getReadablePartitions()
  {
    return readablePartitions;
  }
}
