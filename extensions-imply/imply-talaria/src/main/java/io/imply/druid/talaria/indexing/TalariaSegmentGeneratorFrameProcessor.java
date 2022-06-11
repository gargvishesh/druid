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
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.exec.TalariaTasks;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.segment.FrameStorageAdapter;
import io.imply.druid.talaria.util.FutureUtils;
import io.imply.druid.talaria.util.SequenceUtils;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TalariaSegmentGeneratorFrameProcessor implements FrameProcessor<DataSegment>
{
  private static final Logger log = new Logger(TalariaSegmentGeneratorFrameProcessor.class);

  private final ReadableFrameChannel inChannel;
  private final FrameReader frameReader;
  private final Appenderator appenderator;
  private final SegmentIdWithShardSpec segmentIdWithShardSpec;
  private final List<String> dimensionsForInputRows;
  private final Object2IntMap<String> outputColumnNameToFrameColumnNumberMap;

  private boolean firstRun = true;
  private long rowsWritten = 0L;

  TalariaSegmentGeneratorFrameProcessor(
      final ReadableFrameChannel inChannel,
      final FrameReader frameReader,
      final ColumnMappings columnMappings,
      final List<String> dimensionsForInputRows,
      final Appenderator appenderator,
      final SegmentIdWithShardSpec segmentIdWithShardSpec
  )
  {
    this.inChannel = inChannel;
    this.frameReader = frameReader;
    this.appenderator = appenderator;
    this.segmentIdWithShardSpec = segmentIdWithShardSpec;
    this.dimensionsForInputRows = dimensionsForInputRows;

    outputColumnNameToFrameColumnNumberMap = new Object2IntOpenHashMap<>();
    outputColumnNameToFrameColumnNumberMap.defaultReturnValue(-1);

    for (final ColumnMapping columnMapping : columnMappings.getMappings()) {
      outputColumnNameToFrameColumnNumberMap.put(
          columnMapping.getOutputColumn(),
          frameReader.signature().indexOf(columnMapping.getQueryColumn())
      );
    }
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public ReturnOrAwait<DataSegment> runIncrementally(final IntSet readableInputs)
  {
    if (firstRun) {
      log.debug("Starting job for segment [%s].", segmentIdWithShardSpec.asSegmentId());
      appenderator.startJob();
      firstRun = false;
    }

    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    }

    if (inChannel.isFinished()) {
      if (rowsWritten == 0) {
        log.debug("Finished reading. No data for segment [%s], skipping.", segmentIdWithShardSpec.asSegmentId());
        return ReturnOrAwait.returnObject(null);
      } else {
        log.debug("Finished reading. Pushing segment [%s].", segmentIdWithShardSpec.asSegmentId());

        // TODO(gianm): This blocks, violating FrameProcessor contract
        // TODO(gianm): This doesn't respond properly to task cancellation
        // useUniquePath = false because this class is meant to be used by batch jobs.
        final ListenableFuture<SegmentsAndCommitMetadata> pushFuture =
            appenderator.push(Collections.singletonList(segmentIdWithShardSpec), null, false);
        final SegmentsAndCommitMetadata metadata = FutureUtils.getUnchecked(pushFuture, true);

        try {
          appenderator.clear();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }

        log.debug("Finished work for segment [%s].", segmentIdWithShardSpec.asSegmentId());
        return ReturnOrAwait.returnObject(Iterables.getOnlyElement(metadata.getSegments()));
      }
    } else {
      if (appenderator.getSegments().isEmpty()) {
        log.debug("Received first frame for segment [%s].", segmentIdWithShardSpec.asSegmentId());
      }

      addFrame(inChannel.read().getOrThrow());
      return ReturnOrAwait.awaitAll(1);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels(), appenderator::close);
  }

  private void addFrame(final Frame frame)
  {
    final RowSignature signature = frameReader.signature();

    // Reuse input row to avoid redoing allocations.
    final TalariaInputRow inputRow = new TalariaInputRow();

    final Sequence<Cursor> cursorSequence =
        new FrameStorageAdapter(frame, frameReader, Intervals.ETERNITY)
            .makeCursors(null, Intervals.ETERNITY, VirtualColumns.EMPTY, Granularities.ALL, false, null);

    SequenceUtils.forEach(
        cursorSequence,
        cursor -> {
          final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

          //noinspection rawtypes
          @SuppressWarnings("rawtypes")
          final List<BaseObjectColumnValueSelector> selectors =
              frameReader.signature()
                         .getColumnNames()
                         .stream()
                         .map(columnSelectorFactory::makeColumnValueSelector)
                         .collect(Collectors.toList());

          while (!cursor.isDone()) {
            for (int j = 0; j < signature.size(); j++) {
              inputRow.getBackingArray()[j] = selectors.get(j).getObject();
            }

            try {
              rowsWritten++;
              appenderator.add(segmentIdWithShardSpec, inputRow, null);
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }

            cursor.advance();
          }
        }
    );
  }

  private class TalariaInputRow implements InputRow
  {
    private final Object[] backingArray;
    private final int timeColumnNumber = outputColumnNameToFrameColumnNumberMap.getInt(ColumnHolder.TIME_COLUMN_NAME);

    public TalariaInputRow()
    {
      this.backingArray = new Object[frameReader.signature().size()];
    }

    @Override
    public long getTimestampFromEpoch()
    {
      if (timeColumnNumber < 0) {
        return 0;
      } else {
        return TalariaTasks.primaryTimestampFromObjectForInsert(backingArray[timeColumnNumber]);
      }
    }

    @Override
    public DateTime getTimestamp()
    {
      return DateTimes.utc(getTimestampFromEpoch());
    }

    @Override
    public List<String> getDimensions()
    {
      return dimensionsForInputRows;
    }

    @Nullable
    @Override
    public Object getRaw(String columnName)
    {
      final int columnNumber = outputColumnNameToFrameColumnNumberMap.getInt(columnName);
      if (columnNumber < 0) {
        return null;
      } else {
        return backingArray[columnNumber];
      }
    }

    @Override
    public List<String> getDimension(String columnName)
    {
      return Rows.objectToStrings(getRaw(columnName));
    }

    @Nullable
    @Override
    public Number getMetric(String columnName)
    {
      return Rows.objectToNumber(columnName, getRaw(columnName), true);
    }

    @Override
    public int compareTo(Row other)
    {
      // Not used during indexing.
      throw new UnsupportedOperationException();
    }

    private Object[] getBackingArray()
    {
      return backingArray;
    }

    @Override
    public String toString()
    {
      return "TalariaInputRow{" +
             "backingArray=" + Arrays.toString(backingArray) +
             ", timeColumnNumber=" + timeColumnNumber +
             '}';
    }
  }
}
