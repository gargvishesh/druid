/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import io.imply.druid.talaria.exec.TalaraDataSegmentProvider;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.write.ArenaMemoryAllocator;
import io.imply.druid.talaria.indexing.externalsink.TalariaExternalSink;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentCacheManager;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class IndexerFrameContext implements FrameContext
{
  private final IndexerWorkerContext context;
  private final IndexIO indexIO;
  private final Supplier<MemoryAllocator> allocatorMaker;
  private final TalaraDataSegmentProvider dataSegmentProvider;

  public IndexerFrameContext(IndexerWorkerContext context)
  {
    this.context = context;
    Injector injector = context.injector();
    SegmentCacheManager segmentCacheManager = injector.getInstance(SegmentCacheManagerFactory.class)
        .manufacturate(new File(context.tempDir(), "segment-fetch"));
    this.indexIO = injector.getInstance(IndexIO.class);
    this.dataSegmentProvider = new TalaraDataSegmentProvider(segmentCacheManager, this.indexIO);
    this.allocatorMaker = () ->
        ArenaMemoryAllocator.create(ByteBuffer.allocate(MemoryLimits.FRAME_SIZE));
  }

  @Override
  public Supplier<MemoryAllocator> allocatorMaker()
  {
    return allocatorMaker;
  }

  @Override
  public MemoryAllocator memoryAllocator()
  {
    return allocatorMaker.get();
  }

  @Override
  public JoinableFactory joinableFactory()
  {
    return context.injector().getInstance(JoinableFactory.class);
  }

  @Override
  public GroupByStrategySelector groupByStrategySelector()
  {
    return context.injector().getInstance(GroupByStrategySelector.class);
  }

  @Override
  public RowIngestionMeters rowIngestionMeters()
  {
    return context.toolbox().getRowIngestionMetersFactory().createRowIngestionMeters();
  }

  @Override
  public DataSegmentProvider dataSegmentProvider()
  {
    return dataSegmentProvider;
  }

  @Override
  public File tempDir()
  {
    return context.tempDir();
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return context.jsonMapper();
  }

  @Override
  public TalariaExternalSink externalSink()
  {
    return context.injector().getInstance(TalariaExternalSink.class);
  }

  @Override
  public IndexIO indexIO()
  {
    return indexIO;
  }

  @Override
  public File persistDir()
  {
    return context.toolbox().getPersistDir();
  }

  @Override
  public DataSegmentPusher segmentPusher()
  {
    return context.toolbox().getSegmentPusher();
  }

  @Override
  public IndexMergerV9 indexMerger()
  {
    return context.toolbox().getIndexMergerV9();
  }
}
