/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollector;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.segment.Cursor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class TalariaClusterByStatisticsCollectionProcessor
    implements FrameProcessor<ClusterByStatisticsCollector>
{
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final ClusterBy clusterBy;

  private ClusterByStatisticsCollector clusterByStatisticsCollector;

  public TalariaClusterByStatisticsCollectionProcessor(
      final ReadableFrameChannel inputChannel,
      final WritableFrameChannel outputChannel,
      final FrameReader frameReader,
      final ClusterBy clusterBy,
      final ClusterByStatisticsCollector clusterByStatisticsCollector
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.clusterBy = clusterBy;
    this.clusterByStatisticsCollector = clusterByStatisticsCollector;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inputChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel);
  }

  @Override
  public ReturnOrAwait<ClusterByStatisticsCollector> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    }

    if (inputChannel.isFinished()) {
      return ReturnOrAwait.returnObject(clusterByStatisticsCollector);
    }

    final Frame frame = inputChannel.read().getOrThrow();
    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);
    final Supplier<ClusterByKey> clusterByKeyReader = clusterBy.keyReader(
        cursor.getColumnSelectorFactory(),
        frameReader.signature()
    );

    while (!cursor.isDone()) {
      clusterByStatisticsCollector.add(clusterByKeyReader.get());
      cursor.advance();
    }

    // TODO(gianm): Clears partition info; make sure this is OK
    outputChannel.write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
    return ReturnOrAwait.awaitAll(1);
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(
        inputChannels(),
        outputChannels(),
        () -> clusterByStatisticsCollector = null
    );
  }
}
