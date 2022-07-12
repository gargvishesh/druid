/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.scan;

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
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

@JsonTypeName("scan")
public class ScanQueryFrameProcessorFactory extends BaseLeafFrameProcessorFactory
{
  private final ScanQuery query;

  // TODO(gianm): HACK ALERT: shared limit across all processors
  // TODO(gianm): Bad because nothing really guarantees this factory is only used for a single makeWorkers call
  @Nullable
  private final AtomicLong runningCountForLimit;

  @JsonCreator
  public ScanQueryFrameProcessorFactory(
      @JsonProperty("query") ScanQuery query
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.runningCountForLimit =
        query.isLimited() && query.getOrderBys().isEmpty() ? new AtomicLong() : null;
  }

  @JsonProperty
  public ScanQuery getQuery()
  {
    return query;
  }

  @Override
  protected FrameProcessor<Long> makeProcessor(
      ReadableInput baseInput,
      Int2ObjectMap<ReadableInput> sideChannels,
      ResourceHolder<WritableFrameChannel> outputChannelSupplier,
      ResourceHolder<MemoryAllocator> allocatorSupplier,
      RowSignature signature,
      ClusterBy clusterBy,
      FrameContext frameContext
  )
  {
    return new ScanQueryFrameProcessor(
        query,
        signature,
        clusterBy,
        baseInput,
        sideChannels,
        new JoinableFactoryWrapper(frameContext.joinableFactory()),
        outputChannelSupplier,
        allocatorSupplier,
        runningCountForLimit,
        frameContext.memoryParameters().getBroadcastJoinMemory()
    );
  }
}
