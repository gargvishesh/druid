/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.druid.java.util.common.IAE;

import java.util.Objects;

public class ReadablePartition
{
  private final int stageNumber;
  private final IntSortedSet workerNumbers;
  private final int partitionNumber;

  private ReadablePartition(final int stageNumber, final IntSortedSet workerNumbers, final int partitionNumber)
  {
    this.stageNumber = stageNumber;
    this.workerNumbers = workerNumbers;
    this.partitionNumber = partitionNumber;

    if (workerNumbers.isEmpty()) {
      throw new IAE("Cannot have empty worker set");
    }
  }

  public static ReadablePartition striped(final int stageNumber, final int numWorkers, final int partitionNumber)
  {
    final IntAVLTreeSet workerNumbers = new IntAVLTreeSet();
    for (int i = 0; i < numWorkers; i++) {
      workerNumbers.add(i);
    }

    return new ReadablePartition(stageNumber, workerNumbers, partitionNumber);
  }

  public static ReadablePartition collected(final int stageNumber, final int workerNumber, final int partitionNumber)
  {
    final IntAVLTreeSet workerNumbers = new IntAVLTreeSet();
    workerNumbers.add(workerNumber);

    return new ReadablePartition(stageNumber, workerNumbers, partitionNumber);
  }

  public int getStageNumber()
  {
    return stageNumber;
  }

  public IntSortedSet getWorkerNumbers()
  {
    return workerNumbers;
  }

  public int getPartitionNumber()
  {
    return partitionNumber;
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
    ReadablePartition that = (ReadablePartition) o;
    return stageNumber == that.stageNumber && partitionNumber == that.partitionNumber && Objects.equals(
        workerNumbers,
        that.workerNumbers
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageNumber, workerNumbers, partitionNumber);
  }

  @Override
  public String toString()
  {
    return "ReadablePartition{" +
           "stageNumber=" + stageNumber +
           ", workerNumbers=" + workerNumbers +
           ", partitionNumber=" + partitionNumber +
           '}';
  }
}
