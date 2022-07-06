/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.primitives.Ints;

import javax.annotation.Nonnull;

public class IntegerDeltaEncoderDecoder
{
  private final long minValue;

  public IntegerDeltaEncoderDecoder(long minValue)
  {
    this.minValue = minValue;
  }

  /**
   * we do a defensive copy of the input because we can't be sure if the caller still needs the input
   *
   * @return a new array that is delta-encoded based on the minimum
   */
  @Nonnull
  public int[] encodeDeltas(long[] values, int length)
  {
    int[] deltas = new int[length];

    if (length == 0) {
      return deltas;
    }

    deltas[0] = Ints.checkedCast(values[0] - minValue);

    for (int i = 1; i < deltas.length; i++) {
      deltas[i] = Ints.checkedCast(values[i] - values[i - 1]);
    }

    return deltas;
  }

  /**
   * @param deltas - mutates the input list into the result to avoid extra allocation and copy because current caller
   *               does not need intermediate result
   */
  @Nonnull
  public void decodeDeltas(long[] deltas)
  {
    if (deltas.length == 0) {
      return;
    }

    deltas[0] += minValue;

    for (int i = 1; i < deltas.length; i++) {
      deltas[i] += deltas[i - 1];
    }
  }
}
