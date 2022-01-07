/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.indexing.externalsink.TalariaExternalSink;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;

import java.io.File;
import java.util.function.Supplier;

/**
 * Provides services that frame processors need to do their thing.
 */
public interface FrameContext
{
  Supplier<MemoryAllocator> allocatorMaker();
  MemoryAllocator memoryAllocator();
  JoinableFactory joinableFactory();
  GroupByStrategySelector groupByStrategySelector();
  RowIngestionMeters rowIngestionMeters();
  DataSegmentProvider dataSegmentProvider();
  File tempDir();
  ObjectMapper jsonMapper();
  TalariaExternalSink externalSink();
  IndexIO indexIO();
  File persistDir();
  DataSegmentPusher segmentPusher();
  IndexMergerV9 indexMerger();
}
