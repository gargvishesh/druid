/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

public class ShuffleSpecFactories
{
  private ShuffleSpecFactories()
  {
    // No instantiation.
  }

  public static ShuffleSpecFactory singlePartition()
  {
    return (clusterBy, aggregate) ->
        new MaxCountShuffleSpec(clusterBy, 1, aggregate);
  }

  public static ShuffleSpecFactory subQueryWithMaxWorkerCount(final int maxWorkerCount)
  {
    return (clusterBy, aggregate) ->
        new MaxCountShuffleSpec(clusterBy, maxWorkerCount, aggregate);
  }
}
