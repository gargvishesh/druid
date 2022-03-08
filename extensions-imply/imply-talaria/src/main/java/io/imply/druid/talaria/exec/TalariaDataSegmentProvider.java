/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.indexing.TalariaCounters;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import io.imply.druid.talaria.querykit.LazyResourceHolder;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;

public class TalariaDataSegmentProvider implements DataSegmentProvider
{
  private final SegmentCacheManager segmentCacheManager;
  private final IndexIO indexIO;

  public TalariaDataSegmentProvider(
      SegmentCacheManager segmentCacheManager,
      IndexIO indexIO
  )
  {
    this.segmentCacheManager = segmentCacheManager;
    this.indexIO = indexIO;
  }

  @Override
  public LazyResourceHolder<Segment> fetchSegment(
      final DataSegment dataSegment,
      final TalariaCounters.ChannelCounters channelCounters
  )
  {
    try {
      return new LazyResourceHolder<>(
          () -> {
            try {
              final Closer closer = Closer.create();
              final File segmentDir = segmentCacheManager.getSegmentFiles(dataSegment);
              closer.register(() -> FileUtils.deleteDirectory(segmentDir));

              final QueryableIndex index = indexIO.loadIndex(segmentDir);
              final int numRows = index.getNumRows();
              final long size = dataSegment.getSize();
              closer.register(() ->
                                  channelCounters.addCounters(
                                      0,
                                      numRows,
                                      size,
                                      1
                                  )
              );
              closer.register(index);
              return Pair.of(new QueryableIndexSegment(index, dataSegment.getId()), closer);
            }
            catch (IOException | SegmentLoadingException e) {
              throw new RuntimeException(e);
            }
          }
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
