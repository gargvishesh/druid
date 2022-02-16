/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.scan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.boost.SettableLongVirtualColumn;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameRowTooLargeException;
import io.imply.druid.talaria.frame.processor.MultiColumnSelectorFactory;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.segment.FrameSegment;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.querykit.BaseLeafFrameProcessor;
import io.imply.druid.talaria.querykit.QueryKitUtils;
import io.imply.druid.talaria.querykit.QueryWorkerInput;
import io.imply.druid.talaria.querykit.SegmentWithInterval;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link FrameProcessor} that reads one {@link Frame} at a time from a particular segment, writes them
 * to a {@link WritableFrameChannel}, and returns the number of rows output.
 *
 * TODO(gianm): implement offset
 */
public class ScanQueryFrameProcessor extends BaseLeafFrameProcessor
{
  private final ScanQuery query;
  private final RowSignature signature;
  private final ClusterBy clusterBy;
  private final AtomicLong runningCountForLimit;
  private final SettableLongVirtualColumn partitionBoostVirtualColumn;
  private final ColumnSelectorFactory frameWriterColumnSelectorFactory;
  private final Closer closer = Closer.create();

  private long rowsOutput = 0;
  private Cursor cursor;
  private FrameWriter frameWriter;

  public ScanQueryFrameProcessor(
      final ScanQuery query,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final QueryWorkerInput baseInput,
      final Int2ObjectMap<ReadableFrameChannel> sideChannels,
      final Int2ObjectMap<FrameReader> sideChannelReaders,
      final JoinableFactoryWrapper joinableFactory,
      final ResourceHolder<WritableFrameChannel> outputChannel,
      final ResourceHolder<MemoryAllocator> allocator,
      @Nullable final AtomicLong runningCountForLimit,
      final AtomicLong broadcastHashJoinRhsTablesMemoryCounter
  )
  {
    super(
        query,
        baseInput,
        sideChannels,
        sideChannelReaders,
        joinableFactory,
        outputChannel,
        allocator,
        broadcastHashJoinRhsTablesMemoryCounter
    );
    this.query = query;
    this.signature = signature;
    this.clusterBy = clusterBy;
    this.runningCountForLimit = runningCountForLimit;
    this.partitionBoostVirtualColumn = new SettableLongVirtualColumn(QueryKitUtils.PARTITION_BOOST_COLUMN);

    final List<VirtualColumn> frameWriterVirtualColumns = new ArrayList<>();
    frameWriterVirtualColumns.add(partitionBoostVirtualColumn);

    final VirtualColumn segmentGranularityVirtualColumn =
        QueryKitUtils.makeSegmentGranularityVirtualColumn(
            QueryKitUtils.getSegmentGranularityFromContext(query.getContext()),
            query.getContextValue(QueryKitUtils.CTX_TIME_COLUMN_NAME)
        );

    if (segmentGranularityVirtualColumn != null) {
      frameWriterVirtualColumns.add(segmentGranularityVirtualColumn);
    }

    this.frameWriterColumnSelectorFactory =
        VirtualColumns.create(frameWriterVirtualColumns)
                      .wrap(
                          new MultiColumnSelectorFactory(
                              Collections.singletonList(() -> cursor.getColumnSelectorFactory()),
                              signature
                          )
                      );
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    // "legacy" should be non-null due to toolChest.mergeResults
    final boolean legacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");

    if (legacy) {
      throw new ISE("Cannot use this engine in legacy mode");
    }

    if (runningCountForLimit != null
        && runningCountForLimit.get() > query.getScanRowsOffset() + query.getScanRowsLimit()) {
      return ReturnOrAwait.returnObject(rowsOutput);
    }

    return super.runIncrementally(readableInputs);
  }

  @Override
  public void cleanup() throws IOException
  {
    closer.register(frameWriter);
    closer.register(super::cleanup);
    closer.close();
  }

  @Override
  protected ReturnOrAwait<Long> runWithSegment(final SegmentWithInterval segment) throws IOException
  {
    if (cursor == null) {
      closer.register(segment);

      final Yielder<Cursor> cursorYielder = Yielders.each(
          makeCursors(
              query.withQuerySegmentSpec(new SpecificSegmentSpec(segment.toDescriptor())),
              mapSegment(segment.getOrLoadSegment()).asStorageAdapter()
          )
      );

      if (cursorYielder.isDone()) {
        // No cursors!
        cursorYielder.close();
        return ReturnOrAwait.returnObject(rowsOutput);
      } else {
        cursor = cursorYielder.get();
        closer.register(cursorYielder);
      }
    }

    populateFrameWriterAndFlushIfNeeded();

    if (cursor.isDone()) {
      flushFrameWriterIfNeeded();
    }

    if (cursor.isDone() && (frameWriter == null || frameWriter.getNumRows() == 0)) {
      return ReturnOrAwait.returnObject(rowsOutput);
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  @Override
  protected ReturnOrAwait<Long> runWithInputChannel(
      final ReadableFrameChannel inputChannel,
      final FrameReader inputFrameReader
  ) throws IOException
  {
    if (cursor == null || cursor.isDone()) {
      if (inputChannel.canRead()) {
        final Frame frame = inputChannel.read().getOrThrow();
        final FrameSegment frameSegment = new FrameSegment(frame, inputFrameReader, SegmentId.dummy("x"));

        this.cursor = Iterables.getOnlyElement(
            makeCursors(
                query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY)),
                mapSegment(frameSegment).asStorageAdapter()
            ).toList()
        );
      } else if (inputChannel.isFinished()) {
        flushFrameWriterIfNeeded();
        return ReturnOrAwait.returnObject(rowsOutput);
      } else {
        return ReturnOrAwait.awaitAll(inputChannels().size());
      }
    }

    // Cursor has some more data in it.
    populateFrameWriterAndFlushIfNeeded();

    if (cursor.isDone()) {
      return ReturnOrAwait.awaitAll(inputChannels().size());
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  private void populateFrameWriterAndFlushIfNeeded() throws IOException
  {
    createFrameWriterIfNeeded();

    while (!cursor.isDone()) {
      if (!frameWriter.addSelection()) {
        if (frameWriter.getNumRows() > 0) {
          final long numRowsWritten = flushFrameWriterIfNeeded();

          if (runningCountForLimit != null) {
            runningCountForLimit.addAndGet(numRowsWritten);
          }

          return;
        } else {
          throw new FrameRowTooLargeException();
        }
      }

      cursor.advance();
      partitionBoostVirtualColumn.setValue(partitionBoostVirtualColumn.getValue() + 1);
    }
  }

  private void createFrameWriterIfNeeded()
  {
    if (frameWriter == null) {
      frameWriter = FrameWriter.create(frameWriterColumnSelectorFactory, getAllocator(), signature);
    }
  }

  private long flushFrameWriterIfNeeded() throws IOException
  {
    if (frameWriter != null && frameWriter.getNumRows() > 0) {
      frameWriter.sort(clusterBy.getColumns());
      final Frame frame = Frame.wrap(frameWriter.toByteArray());
      Iterables.getOnlyElement(outputChannels()).write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
      frameWriter.close();
      frameWriter = null;
      rowsOutput += frame.numRows();
      return frame.numRows();
    } else {
      return 0;
    }
  }

  private static Sequence<Cursor> makeCursors(final ScanQuery query, final StorageAdapter adapter)
  {
    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));

    return adapter.makeCursors(
        filter,
        intervals.get(0),
        query.getVirtualColumns(),
        Granularities.ALL,
        ScanQuery.Order.DESCENDING.equals(query.getTimeOrder()),
        null
    );
  }
}
