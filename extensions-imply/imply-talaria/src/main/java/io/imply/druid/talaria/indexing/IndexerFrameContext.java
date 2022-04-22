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
import io.imply.druid.talaria.exec.WorkerMemoryParameters;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.indexing.externalsink.TalariaExternalSink;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;

import java.io.File;

public class IndexerFrameContext implements FrameContext
{
  private final IndexerWorkerContext context;
  private final IndexIO indexIO;
  private final DataSegmentProvider dataSegmentProvider;
  private final WorkerMemoryParameters memoryParameters;

  public IndexerFrameContext(
      IndexerWorkerContext context,
      IndexIO indexIO,
      DataSegmentProvider dataSegmentProvider,
      WorkerMemoryParameters memoryParameters
  )
  {
    this.context = context;
    this.indexIO = indexIO;
    this.dataSegmentProvider = dataSegmentProvider;
    this.memoryParameters = memoryParameters;
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

  @Override
  public WorkerMemoryParameters memoryParameters()
  {
    return memoryParameters;
  }
}
