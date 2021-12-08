/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.List;

/**
 * A FrameProcessor is like an incremental version of Runnable that operates on {@link ReadableFrameChannel} and
 * {@link WritableFrameChannel}.
 *
 * It is designed to enable interleaved non-blocking work on a fixed-size thread pool. Typically, this is done using
 * an instance of {@link FrameProcessorExecutor}.
 */
public interface FrameProcessor<T>
{
  List<ReadableFrameChannel> inputChannels();

  List<WritableFrameChannel> outputChannels();

  /**
   * Runs a little bit of the algorithm, without blocking, and either returns a value or a set of input channels
   * to wait for. This method is called by {@link FrameProcessorExecutor#runFully} when all output channels are
   * writable. Therefore, it is guaranteed that each output channel can accept at least one frame.
   *
   * @param readableInputs channels from {@link #inputChannels()} that are either finished or ready to read.
   *
   * @return either a final return value or a set of input channels to wait for. Must be nonnull.
   */
  ReturnOrAwait<T> runIncrementally(IntSet readableInputs) throws IOException;

  /**
   * Cleans up resources used by this worker, including signalling to input and output channels that we are
   * done reading and writing, via {@link ReadableFrameChannel#doneReading()} and
   * {@link WritableFrameChannel#doneWriting()}.
   *
   * This method may be called before the worker reports completion via {@link #runIncrementally}, especially in
   * cases of cancellation.
   */
  void cleanup() throws IOException;
}
