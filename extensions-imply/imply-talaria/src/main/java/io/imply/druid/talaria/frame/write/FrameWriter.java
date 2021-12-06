/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.AppendableMemory;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.MemoryWithRange;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FrameWriter implements Closeable
{
  public static final byte VERSION_ONE_MAGIC = 0x11;

  public static final long HEADER_SIZE =
      Byte.BYTES /* version */ +
      Long.BYTES /* total size */ +
      Integer.BYTES /* number of rows */ +
      Integer.BYTES /* number of columns */ +
      Byte.BYTES /* permuted flag */;

  public static final LZ4Compressor LZ4_COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();

  private final AppendableMemory rowOrderWriter;
  private final List<FrameColumnWriter> columnWriters;
  private final Object2IntMap<String> columnNameToPosition;
  private int numRows = 0;
  private boolean permuted = false;

  private FrameWriter(
      final AppendableMemory rowOrderWriter,
      final List<FrameColumnWriter> columnWriters,
      final Object2IntMap<String> columnNameToPosition
  )
  {
    this.rowOrderWriter = rowOrderWriter;
    this.columnWriters = columnWriters;
    this.columnNameToPosition = columnNameToPosition;
  }

  /**
   * Create a FrameWriter.
   *
   * @param columnSelectorFactory input for this frame writer
   * @param allocator             memory allocator; will use as much as possible
   * @param signature             output signature for this frame writer
   *
   * @throws UnsupportedColumnTypeException if "signature" contains any type that we cannot handle
   */
  public static FrameWriter create(
      final ColumnSelectorFactory columnSelectorFactory,
      final MemoryAllocator allocator,
      final RowSignature signature
  )
  {
    final List<FrameColumnWriter> columnWriters = new ArrayList<>();
    final Object2IntOpenHashMap<String> columnNameToPosition = new Object2IntOpenHashMap<>();
    columnNameToPosition.defaultReturnValue(-1);

    for (int i = 0; i < signature.size(); i++) {
      final String column = signature.getColumnName(i);
      // note: null type won't work, but we'll get a nice error from FrameColumnWriters.create
      final ColumnType columnType = signature.getColumnType(i).orElse(null);
      columnNameToPosition.put(column, i);
      columnWriters.add(FrameColumnWriters.create(columnSelectorFactory, allocator, column, columnType));
    }

    return new FrameWriter(AppendableMemory.create(allocator), columnWriters, columnNameToPosition);
  }

  /**
   * Write the current row to the frame that is under construction, if there is enough space to do so.
   *
   * If this method returns false on an empty frame, or in a situation where starting a new frame is impractical,
   * it is conventional (although not required) for the caller to throw
   * {@link io.imply.druid.talaria.frame.processor.FrameRowTooLargeException}.
   *
   * @return true if the row was written, false if there was not enough space
   */
  public boolean addSelection()
  {
    if (permuted) {
      throw new ISE("Cannot modify after sorting");
    }

    if (numRows == Integer.MAX_VALUE) {
      return false;
    }

    if (!rowOrderWriter.reserve(Integer.BYTES)) {
      return false;
    }

    int i = 0;
    for (; i < columnWriters.size(); i++) {
      if (!columnWriters.get(i).addSelection()) {
        break;
      }
    }

    if (i < columnWriters.size()) {
      // Add failed, clean up.
      for (int j = 0; j < i; j++) {
        columnWriters.get(j).undo();
      }

      return false;
    } else {
      final MemoryWithRange<WritableMemory> rowOrderCursor = rowOrderWriter.cursor();
      rowOrderCursor.memory().putInt(rowOrderCursor.start(), numRows);
      rowOrderWriter.advanceCursor(Integer.BYTES);
      numRows++;

      return true;
    }
  }

  /**
   * TODO(gianm): Call out that once this is called, you can't add more stuff.
   * TODO(gianm): Also call out that if you pass an empty list, nothing happens.
   * TODO(gianm): permuted sort may be a bad idea; doesn't play well with compression; will need for large spilled sets
   */
  public void sort(final List<ClusterByColumn> clusterByColumns)
  {
    if (permuted) {
      throw new ISE("Cannot modify after sorting");
    }

    if (!clusterByColumns.isEmpty()) {
      final int[] sortByPositions = new int[clusterByColumns.size()];
      final int[] sortByMultipliers = new int[clusterByColumns.size()];

      for (int i = 0; i < clusterByColumns.size(); i++) {
        final String column = clusterByColumns.get(i).columnName();
        // TODO(gianm): Some handling or test for column not found
        sortByPositions[i] = columnNameToPosition.getInt(column);
        sortByMultipliers[i] = clusterByColumns.get(i).descending() ? -1 : 1;
      }

      @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
      final List<Integer> wrappedRowOrderData = new AbstractList<Integer>()
      {
        @Override
        public Integer get(int index)
        {
          return rowOrderWriter.getInt((long) index * Integer.BYTES);
        }

        @Override
        public Integer set(int index, Integer element)
        {
          final MemoryWithRange<WritableMemory> memoryWithRange =
              rowOrderWriter.read((long) index * Integer.BYTES);

          final int oldValue = memoryWithRange.memory().getInt(memoryWithRange.start());
          memoryWithRange.memory().putInt(memoryWithRange.start(), element);
          return oldValue;
        }

        @Override
        public int size()
        {
          return numRows;
        }
      };

      // TODO(gianm): By default this allocates a new array, sorts it, then copies back to the collection. we can do better.
      Collections.sort(
          wrappedRowOrderData,
          (row1, row2) -> {
            for (int i = 0; i < sortByPositions.length; i++) {
              int sortByPosition = sortByPositions[i];
              final int cmp = columnWriters.get(sortByPosition).compare(row1, row2);

              if (cmp != 0) {
                return cmp * sortByMultipliers[i];
              }
            }

            return 0;
          }
      );

      permuted = true;
    }
  }

  public int getNumRows()
  {
    return numRows;
  }

  /**
   * TODO(gianm): Javadocs, including the fact that it returns bytes written.
   */
  public long writeTo(final WritableByteChannel channel, final boolean compress) throws IOException
  {
    // TODO(gianm): Checksum somewhere?

    if (compress) {
      // TODO(gianm): Limit allocations somehow
      // TODO(gianm): Clear stuff as it's copied through this pipeline? To prevent having so many stuff at once:
      //   - appendable memory
      //   - raw frame
      //   - compressed frame
      final byte[] frameBytes = toByteArray();
      final byte[] compressedFrameBytes = LZ4_COMPRESSOR.compress(frameBytes);
      Channels.writeFully(channel, ByteBuffer.wrap(compressedFrameBytes));
      return compressedFrameBytes.length;
    }

    final long totalSize = computeTotalSize();

    // Write header.
    final ByteBuffer tmp = ByteBuffer.allocate(Ints.checkedCast(HEADER_SIZE)).order(ByteOrder.nativeOrder());
    tmp.put(VERSION_ONE_MAGIC)
       .putLong(totalSize)
       .putInt(numRows)
       .putInt(columnWriters.size())
       .put(permuted ? (byte) 1 : (byte) 0)
       .flip();
    Channels.writeFully(channel, tmp);

    if (permuted) {
      rowOrderWriter.writeTo(channel);
    }

    // Write column ending positions.
    long columnEndPosition = HEADER_SIZE + computeRowOrderSize() + computeColumnEndSize();

    for (FrameColumnWriter writer : columnWriters) {
      columnEndPosition += writer.size();
      tmp.clear();
      tmp.putLong(columnEndPosition).flip();
      Channels.writeFully(channel, tmp);
    }

    // Write columns.
    for (FrameColumnWriter writer : columnWriters) {
      writer.writeTo(channel);
    }

    return totalSize;
  }

  public byte[] toByteArray()
  {
    try {
      // TODO(gianm): Ensure that the size fits into an int, somehow
      final byte[] bytes = new byte[Ints.checkedCast(computeTotalSize())];
      final long bytesWritten = writeTo(
          new WritableByteChannel()
          {
            int pos = 0;

            @Override
            public int write(ByteBuffer src)
            {
              final int len = src.remaining();
              src.get(bytes, pos, len);
              pos += len;
              return len;
            }

            @Override
            public boolean isOpen()
            {
              return true;
            }

            @Override
            public void close()
            {
              // Nothing to do.
            }
          },
          false
      );

      // Sanity check
      if (bytesWritten != bytes.length) {
        throw new ISE("Expected [%,d] bytes but got [%,d] bytes", bytes.length, bytesWritten);
      }

      return bytes;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close()
  {
    rowOrderWriter.close();
    columnWriters.forEach(FrameColumnWriter::close);
  }

  private long computeRowOrderSize()
  {
    // TODO(gianm): javadoc
    return permuted ? rowOrderWriter.size() : 0;
  }

  private long computeColumnEndSize()
  {
    // TODO(gianm): javadoc
    return (long) columnWriters.size() * Long.BYTES;
  }

  private long computeDataSize()
  {
    // TODO(gianm): javadoc
    return columnWriters.stream().mapToLong(FrameColumnWriter::size).sum();
  }

  private long computeTotalSize()
  {
    // TODO(gianm): javadoc
    return HEADER_SIZE + computeRowOrderSize() + computeColumnEndSize() + computeDataSize();
  }
}
