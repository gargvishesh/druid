/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.collect.Iterables;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.BlockingQueueFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameChannelMerger;
import io.imply.druid.talaria.frame.processor.FrameChannelMuxer;
import io.imply.druid.talaria.frame.processor.FrameProcessorExecutor;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.write.FrameWriters;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.ReadablePartition;
import io.imply.druid.talaria.kernel.ReadablePartitions;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.StagePartition;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntSortedMap;
import org.apache.druid.java.util.common.IAE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class InputChannels
{
  private static final int NO_STAGE_PARTITION = -1;

  private final List<StagePartition> stagePartitionList;
  private final Object2IntSortedMap<StagePartition> stagePartitionIndex;

  // One of each of the following per StagePartition; use stagePartitionIndex to find the appropriate index.
  private final List<ChannelSupplier> channelSuppliers;
  private final List<FrameReader> channelFrameReaders;

  private InputChannels(
      final Object2IntSortedMap<StagePartition> stagePartitionIndex,
      final List<ChannelSupplier> channelSuppliers,
      final List<FrameReader> channelFrameReaders
  )
  {
    this.stagePartitionIndex = stagePartitionIndex;
    this.channelSuppliers = channelSuppliers;
    this.channelFrameReaders = channelFrameReaders;

    stagePartitionList = new ArrayList<>(stagePartitionIndex.size());
    stagePartitionList.addAll(stagePartitionIndex.keySet());
  }

  public static InputChannels create(
      final QueryDefinition queryDefinition,
      final int[] inputStageNumbers,
      final ReadablePartitions inputPartitions,
      final InputChannelFactory channelFactory,
      final Supplier<MemoryAllocator> allocatorMaker,
      final FrameProcessorExecutor exec,
      final String cancellationId
  )
  {
    final Map<Integer, StageDefinition> stageDefinitionMap = new HashMap<>();

    for (int stageNumber : inputStageNumbers) {
      stageDefinitionMap.put(stageNumber, queryDefinition.getStageDefinition(stageNumber));
    }

    final Object2IntSortedMap<StagePartition> stagePartitionIndex = new Object2IntRBTreeMap<>();
    stagePartitionIndex.defaultReturnValue(NO_STAGE_PARTITION);

    final List<ChannelSupplier> channelSuppliers = new ArrayList<>();
    final List<FrameReader> channelFrameReaders = new ArrayList<>();
    final Map<StageId, FrameReader> stageFrameReaderMap = new HashMap<>();

    for (final ReadablePartition inputPartition : inputPartitions) {
      final StageDefinition inputStageDefinition = stageDefinitionMap.get(inputPartition.getStageNumber());
      final ClusterBy inputClusterBy = queryDefinition.getClusterByForStage(inputStageDefinition.getStageNumber());
      final StagePartition inputStagePartition = new StagePartition(
          inputStageDefinition.getId(),
          inputPartition.getPartitionNumber()
      );

      if (stagePartitionIndex.put(inputStagePartition, stagePartitionIndex.size()) != NO_STAGE_PARTITION) {
        // This stagePartition must have already been added.
        throw new IAE(
            "Cannot add stage [%s], partition [%s] more than once",
            inputStageDefinition.getId(),
            inputPartition.getPartitionNumber()
        );
      }

      if (inputClusterBy.getBucketByCount() == inputClusterBy.getColumns().size()) {
        channelSuppliers.add(
            () ->
                openUnsorted(
                    inputStageDefinition,
                    inputPartition,
                    channelFactory,
                    exec,
                    cancellationId
                )
        );
      } else {
        channelSuppliers.add(
            () ->
                openSorted(
                    inputStageDefinition,
                    inputPartition,
                    inputClusterBy,
                    channelFactory,
                    allocatorMaker,
                    exec,
                    cancellationId
                )
        );
      }

      channelFrameReaders.add(
          stageFrameReaderMap.computeIfAbsent(
              inputStageDefinition.getId(),
              ignored -> FrameReader.create(inputStageDefinition.getSignature())
          )
      );
    }

    return new InputChannels(stagePartitionIndex, channelSuppliers, channelFrameReaders);
  }

  public List<StagePartition> getStagePartitions()
  {
    return stagePartitionList;
  }

  public ReadableFrameChannel openChannel(final StagePartition stagePartition) throws IOException
  {
    final int idx = getStagePartitionIndexOrThrow(stagePartition);
    return channelSuppliers.get(idx).open();
  }

  public FrameReader getFrameReader(final StagePartition stagePartition)
  {
    final int idx = getStagePartitionIndexOrThrow(stagePartition);
    return channelFrameReaders.get(idx);
  }

  private int getStagePartitionIndexOrThrow(final StagePartition stagePartition)
  {
    final int idx = stagePartitionIndex.getInt(stagePartition);

    if (idx != NO_STAGE_PARTITION) {
      return idx;
    } else {
      throw new IAE(
          "No channel for stage [%s], partition [%s]",
          stagePartition.getStageId(),
          stagePartition.getPartitionNumber()
      );
    }
  }

  private static ReadableFrameChannel openUnsorted(
      final StageDefinition stageDefinition,
      final ReadablePartition readablePartition,
      final InputChannelFactory channelFactory,
      final FrameProcessorExecutor exec,
      final String cancellationId
  ) throws IOException
  {
    final List<ReadableFrameChannel> channels = openChannels(
        stageDefinition.getId(),
        readablePartition,
        channelFactory
    );

    if (channels.size() == 1) {
      return Iterables.getOnlyElement(channels);
    } else {
      final BlockingQueueFrameChannel queueChannel = BlockingQueueFrameChannel.minimal();
      final FrameChannelMuxer muxer = new FrameChannelMuxer(channels, queueChannel);

      // TODO(gianm): Return future is ignored. is that ok? can we just use the channel?
      exec.runFully(muxer, cancellationId);

      return queueChannel;
    }
  }

  private static ReadableFrameChannel openSorted(
      final StageDefinition stageDefinition,
      final ReadablePartition readablePartition,
      final ClusterBy clusterBy,
      final InputChannelFactory channelFactory,
      final Supplier<MemoryAllocator> allocatorMaker,
      final FrameProcessorExecutor exec,
      final String cancellationId
  ) throws IOException
  {
    // TODO(gianm): Use SuperSorter, but must implement "already-sorted channel" optimization first.
    final BlockingQueueFrameChannel queueChannel = BlockingQueueFrameChannel.minimal();

    final List<ReadableFrameChannel> channels = openChannels(
        stageDefinition.getId(),
        readablePartition,
        channelFactory
    );

    if (channels.size() == 1) {
      return Iterables.getOnlyElement(channels);
    } else {
      final FrameChannelMerger merger = new FrameChannelMerger(
          channels,
          stageDefinition.getFrameReader(),
          queueChannel,
          FrameWriters.makeFrameWriterFactory(
              FrameType.ROW_BASED,
              allocatorMaker.get(),
              stageDefinition.getFrameReader().signature(),
              Collections.emptyList()
          ),
          clusterBy,
          null,
          -1
      );

      // TODO(gianm): Return future is ignored. is that ok? can we just use the channel?
      exec.runFully(merger, cancellationId);

      return queueChannel;
    }
  }

  private static List<ReadableFrameChannel> openChannels(
      final StageId stageId,
      final ReadablePartition readablePartition,
      final InputChannelFactory channelFactory
  ) throws IOException
  {
    final List<ReadableFrameChannel> channels = new ArrayList<>();

    try {
      for (final int workerNumber : readablePartition.getWorkerNumbers()) {
        channels.add(
            channelFactory.openChannel(
                stageId,
                workerNumber,
                readablePartition.getPartitionNumber()
            )
        );
      }

      return channels;
    }
    catch (Exception e) {
      // Close all channels opened so far before throwing the exception.
      for (final ReadableFrameChannel channel : channels) {
        try {
          channel.doneReading();
        }
        catch (Exception e2) {
          e.addSuppressed(e2);
        }
      }

      throw e;
    }
  }

  private interface ChannelSupplier
  {
    ReadableFrameChannel open() throws IOException;
  }
}
