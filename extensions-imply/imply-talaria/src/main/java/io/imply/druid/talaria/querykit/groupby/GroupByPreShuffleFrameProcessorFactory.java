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
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.querykit.BaseLeafFrameProcessorFactory;
import io.imply.druid.talaria.querykit.QueryWorkerInput;
import io.imply.druid.talaria.querykit.QueryWorkerInputSpec;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

import java.util.List;

@JsonTypeName("groupByPreShuffle")
public class GroupByPreShuffleFrameProcessorFactory extends BaseLeafFrameProcessorFactory
{
  private final GroupByQuery query;
  private final List<QueryWorkerInputSpec> inputSpecs;

  @JsonCreator
  public GroupByPreShuffleFrameProcessorFactory(
      @JsonProperty("query") GroupByQuery query,
      @JsonProperty("inputs") List<QueryWorkerInputSpec> inputSpecs
  )
  {
    super(inputSpecs);
    this.query = Preconditions.checkNotNull(query, "query");
    this.inputSpecs = Preconditions.checkNotNull(inputSpecs, "inputSpecs");
  }

  @JsonProperty
  public GroupByQuery getQuery()
  {
    return query;
  }

  @JsonProperty("inputs")
  public List<QueryWorkerInputSpec> getInputSpecs()
  {
    return inputSpecs;
  }

  @Override
  protected GroupByPreShuffleFrameProcessor makeProcessor(
      final QueryWorkerInput baseInput,
      final Int2ObjectMap<ReadableFrameChannel> sideChannels,
      final Int2ObjectMap<FrameReader> sideChannelReaders,
      final ResourceHolder<WritableFrameChannel> outputChannel,
      final ResourceHolder<MemoryAllocator> allocator,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final FrameContext providerThingy
  )
  {
    return new GroupByPreShuffleFrameProcessor(
        query,
        baseInput,
        sideChannels,
        sideChannelReaders,
        providerThingy.groupByStrategySelector(),
        new JoinableFactoryWrapper(providerThingy.joinableFactory()),
        signature,
        clusterBy,
        outputChannel,
        allocator
    );
  }
}
