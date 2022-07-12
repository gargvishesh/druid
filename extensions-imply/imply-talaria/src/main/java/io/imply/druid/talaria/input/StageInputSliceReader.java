/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.google.common.collect.Iterators;
import io.imply.druid.talaria.counters.CounterNames;
import io.imply.druid.talaria.counters.CounterTracker;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.indexing.CountingReadableFrameChannel;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.StagePartition;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Reads slices of type {@link StageInputSlice}.
 */
public class StageInputSliceReader implements InputSliceReader
{
  private final String queryId;
  private final InputChannels inputChannels;

  public StageInputSliceReader(String queryId, InputChannels inputChannels)
  {
    this.queryId = queryId;
    this.inputChannels = inputChannels;
  }

  @Override
  public int numReadableInputs(final InputSlice slice)
  {
    final StageInputSlice stageInputSlice = (StageInputSlice) slice;

    int count = 0;

    for (final StagePartition stagePartition : inputChannels.getStagePartitions()) {
      if (stagePartition.getStageId().getStageNumber() == stageInputSlice.getStageNumber()) {
        count++;
      }
    }

    return count;
  }

  @Override
  public ReadableInputs attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final StageInputSlice stageInputSlice = (StageInputSlice) slice;
    final StageId stageId = new StageId(queryId, stageInputSlice.getStageNumber());
    final FrameReader frameReader = inputChannels.getFrameReaderForStage(stageInputSlice.getStageNumber());

    return ReadableInputs.channels(
        () -> Iterators.transform(
            stageInputSlice.getPartitions().iterator(),
            partition -> {
              final StagePartition stagePartition = new StagePartition(stageId, partition.getPartitionNumber());

              try {
                return ReadableInput.channel(
                    new CountingReadableFrameChannel(
                        inputChannels.openChannel(stagePartition),
                        counters.channel(CounterNames.inputChannel(inputNumber)),
                        stagePartition.getPartitionNumber()
                    ),
                    frameReader,
                    stagePartition
                );
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
        ),
        frameReader
    );
  }
}
