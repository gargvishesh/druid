/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.input.ReadableInput;
import io.imply.druid.talaria.querykit.BaseLeafFrameProcessorFactory;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

@JsonTypeName("groupByPreShuffle")
public class GroupByPreShuffleFrameProcessorFactory extends BaseLeafFrameProcessorFactory
{
  private final GroupByQuery query;

  @JsonCreator
  public GroupByPreShuffleFrameProcessorFactory(@JsonProperty("query") GroupByQuery query)
  {
    this.query = Preconditions.checkNotNull(query, "query");
  }

  @JsonProperty
  public GroupByQuery getQuery()
  {
    return query;
  }

  @Override
  protected FrameProcessor<Long> makeProcessor(
      final ReadableInput baseInput,
      final Int2ObjectMap<ReadableInput> sideChannels,
      final ResourceHolder<WritableFrameChannel> outputChannelSupplier,
      final ResourceHolder<MemoryAllocator> allocatorSupplier,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final FrameContext frameContext
  )
  {
    return new GroupByPreShuffleFrameProcessor(
        query,
        baseInput,
        sideChannels,
        frameContext.groupByStrategySelector(),
        new JoinableFactoryWrapper(frameContext.joinableFactory()),
        signature,
        clusterBy,
        outputChannelSupplier,
        allocatorSupplier,
        frameContext.memoryParameters().getBroadcastJoinMemory()
    );
  }
}
