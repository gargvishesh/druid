/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import org.apache.druid.java.util.common.StringUtils;

import java.util.Comparator;
import java.util.Objects;

public class StagePartition implements Comparable<StagePartition>
{
  // StagePartition is Comparable because it is used as a sorted map key in InputChannels.
  private static final Comparator<StagePartition> COMPARATOR =
      Comparator.comparing(StagePartition::getStageId)
                .thenComparing(StagePartition::getPartitionNumber);

  private final StageId stageId;
  private final int partitionNumber;

  public StagePartition(StageId stageId, int partitionNumber)
  {
    this.stageId = stageId;
    this.partitionNumber = partitionNumber;
  }

  public StageId getStageId()
  {
    return stageId;
  }

  public int getPartitionNumber()
  {
    return partitionNumber;
  }

  @Override
  public int compareTo(final StagePartition that)
  {
    return COMPARATOR.compare(this, that);
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
    StagePartition that = (StagePartition) o;
    return partitionNumber == that.partitionNumber && Objects.equals(stageId, that.stageId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageId, partitionNumber);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s_%s", stageId, partitionNumber);
  }
}
