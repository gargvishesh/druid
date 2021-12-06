/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame;

import io.imply.druid.talaria.frame.boost.SettableLongVirtualColumn;
import io.imply.druid.talaria.frame.channel.FrameChannelSequence;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.file.FrameFileWriter;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.segment.FrameStorageAdapter;
import io.imply.druid.talaria.util.SequenceUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FrameTestUtil
{
  public static final String ROW_NUMBER_COLUMN = "__row_number";

  private FrameTestUtil()
  {
    // No instantiation.
  }

  public static File writeFrameFile(final Sequence<Frame> frames, final File file) throws IOException
  {
    try (final FrameFileWriter writer = FrameFileWriter.open(Channels.newChannel(new FileOutputStream(file)))) {
      SequenceUtils.forEach(
          frames,
          frame -> {
            try {
              writer.writeFrame(frame, FrameWithPartition.NO_PARTITION);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
    }

    return file;
  }

  public static File writeFrameFileWithPartitions(
      final Sequence<FrameWithPartition> framesWithPartitions,
      final File file
  ) throws IOException
  {
    try (final FrameFileWriter writer = FrameFileWriter.open(Channels.newChannel(new FileOutputStream(file)))) {
      SequenceUtils.forEach(
          framesWithPartitions,
          frameWithPartition -> {
            try {
              writer.writeFrame(frameWithPartition.frame(), frameWithPartition.partition());
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
    }

    return file;
  }

  public static void assertRowsEqual(final Sequence<List<Object>> expected, final Sequence<List<Object>> actual)
  {
    final List<List<Object>> expectedRows = expected.toList();
    final List<List<Object>> actualRows = actual.toList();

    Assert.assertEquals("number of rows", expectedRows.size(), actualRows.size());

    for (int i = 0; i < expectedRows.size(); i++) {
      Assert.assertEquals("row #" + i, expectedRows.get(i), actualRows.get(i));
    }
  }

  /**
   * Reads a sequence of rows from a frame channel using a non-vectorized cursor from
   * {@link FrameStorageAdapter#makeCursors}.
   *
   * @param channel     the channel
   * @param frameReader reader for this channel
   */
  public static Sequence<List<Object>> readRowsFromFrameChannel(
      final ReadableFrameChannel channel,
      final FrameReader frameReader
  )
  {
    return new FrameChannelSequence(channel)
        .flatMap(
            frame ->
                new FrameStorageAdapter(frame, frameReader, Intervals.ETERNITY)
                    .makeCursors(null, Intervals.ETERNITY, VirtualColumns.EMPTY, Granularities.ALL, false, null)
                    .flatMap(cursor -> readRowsFromCursor(cursor, frameReader.signature()))
        );
  }

  /**
   * Reads a sequence of rows from a storage adapter.
   *
   * If {@param populateRowNumberIfPresent} is set, and the provided signature contains {@link #ROW_NUMBER_COLUMN},
   * then that column will be populated with a row number from the adapter.
   *
   * @param adapter           the adapter
   * @param signature         optional signature for returned rows; will use {@code adapter.rowSignature()} if null
   * @param populateRowNumber whether to populate {@link #ROW_NUMBER_COLUMN}
   */
  public static Sequence<List<Object>> readRowsFromAdapter(
      final StorageAdapter adapter,
      @Nullable final RowSignature signature,
      final boolean populateRowNumber
  )
  {
    final RowSignature signatureToUse = signature == null ? adapter.getRowSignature() : signature;
    return makeCursorsForAdapter(adapter, populateRowNumber).flatMap(
        cursor -> readRowsFromCursor(cursor, signatureToUse)
    );
  }

  /**
   * Creates a single-Cursor Sequence from a storage adapter.
   *
   * If {@param populateRowNumber} is set, the row number will be populated into {@link #ROW_NUMBER_COLUMN}.
   *
   * @param adapter           the adapter
   * @param populateRowNumber whether to populate {@link #ROW_NUMBER_COLUMN}
   */
  public static Sequence<Cursor> makeCursorsForAdapter(
      final StorageAdapter adapter,
      final boolean populateRowNumber
  )
  {
    final SettableLongVirtualColumn rowNumberVirtualColumn;
    final VirtualColumns virtualColumns;

    if (populateRowNumber) {
      rowNumberVirtualColumn = new SettableLongVirtualColumn(ROW_NUMBER_COLUMN);
      virtualColumns = VirtualColumns.create(Collections.singletonList(rowNumberVirtualColumn));
    } else {
      rowNumberVirtualColumn = null;
      virtualColumns = VirtualColumns.EMPTY;
    }

    return adapter.makeCursors(null, Intervals.ETERNITY, virtualColumns, Granularities.ALL, false, null)
                  .map(cursor -> {
                    if (populateRowNumber) {
                      return new RowNumberUpdatingCursor(cursor, rowNumberVirtualColumn);
                    } else {
                      return cursor;
                    }
                  });
  }

  private static Sequence<List<Object>> readRowsFromCursor(final Cursor cursor, final RowSignature signature)
  {
    final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

    //noinspection rawtypes
    final List<BaseObjectColumnValueSelector> selectors =
        signature.getColumnNames()
                 .stream()
                 .map(columnSelectorFactory::makeColumnValueSelector)
                 .collect(Collectors.toList());

    final List<List<Object>> retVal = new ArrayList<>();
    while (!cursor.isDone()) {
      final List<Object> o =
          selectors.stream()
                   .map(BaseObjectColumnValueSelector::getObject)
                   .collect(Collectors.toList());

      retVal.add(o);
      cursor.advance();
    }

    return Sequences.simple(retVal);
  }
}
