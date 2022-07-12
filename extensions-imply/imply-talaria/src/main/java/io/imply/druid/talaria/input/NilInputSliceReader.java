/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import io.imply.druid.talaria.counters.CounterTracker;

import java.util.Collections;
import java.util.function.Consumer;

/**
 * Reads slices of type {@link NilInputSlice}.
 */
public class NilInputSliceReader implements InputSliceReader
{
  public static final NilInputSliceReader INSTANCE = new NilInputSliceReader();

  private NilInputSliceReader()
  {
    // Singleton.
  }

  @Override
  public int numReadableInputs(InputSlice slice)
  {
    return 0;
  }

  @Override
  public ReadableInputs attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    return ReadableInputs.segments(Collections.emptyList());
  }
}
