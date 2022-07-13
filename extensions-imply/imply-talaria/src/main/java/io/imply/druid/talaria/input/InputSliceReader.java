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

import java.util.function.Consumer;

/**
 * Reads {@link InputSlice} on workers.
 */
public interface InputSliceReader
{
  /**
   * Returns the number of {@link ReadableInput} that would result from a call to {@link #attach}.
   *
   * @throws UnsupportedOperationException if this reader does not support this spec
   */
  int numReadableInputs(InputSlice slice);

  /**
   * Returns an iterable sequence of {@link ReadableInput} for an {@link InputSpec}, bundled with a
   * {@link io.imply.druid.talaria.frame.read.FrameReader} if appropriate.
   *
   * @throws UnsupportedOperationException if this reader does not support this spec
   */
  ReadableInputs attach(
      int inputNumber,
      InputSlice slice,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  );
}
