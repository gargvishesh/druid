/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.counters.ChannelCounters;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import io.imply.druid.talaria.querykit.LazyResourceHolder;
import io.imply.druid.talaria.rpc.CoordinatorServiceClient;
import org.apache.druid.common.guava.FutureUtils;
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
import org.apache.druid.timeline.SegmentId;

import java.io.File;
import java.io.IOException;

public class TaskDataSegmentProvider implements DataSegmentProvider
{
  private final CoordinatorServiceClient coordinatorClient;
  private final SegmentCacheManager segmentCacheManager;
  private final IndexIO indexIO;

  public TaskDataSegmentProvider(
      CoordinatorServiceClient coordinatorClient,
      SegmentCacheManager segmentCacheManager,
      IndexIO indexIO
  )
  {
    this.coordinatorClient = coordinatorClient;
    this.segmentCacheManager = segmentCacheManager;
    this.indexIO = indexIO;
  }

  @Override
  public LazyResourceHolder<Segment> fetchSegment(
      final SegmentId segmentId,
      final ChannelCounters channelCounters
  )
  {
    try {
      // Use LazyResourceHolder so Coordinator call and segment downloads happen in processing threads,
      // rather than the main thread.
      return new LazyResourceHolder<>(
          () -> {
            try {
              final DataSegment dataSegment = FutureUtils.getUnchecked(
                  coordinatorClient.fetchUsedSegment(
                      segmentId.getDataSource(),
                      segmentId.toString()
                  ),
                  true
              );

              final Closer closer = Closer.create();
              final File segmentDir = segmentCacheManager.getSegmentFiles(dataSegment);
              closer.register(() -> FileUtils.deleteDirectory(segmentDir));

              final QueryableIndex index = indexIO.loadIndex(segmentDir);
              final int numRows = index.getNumRows();
              final long size = dataSegment.getSize();
              closer.register(() -> channelCounters.addFile(numRows, size));
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
