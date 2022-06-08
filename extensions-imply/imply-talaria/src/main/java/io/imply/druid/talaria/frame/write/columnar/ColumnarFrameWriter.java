/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write.columnar;

import io.imply.druid.talaria.frame.AppendableMemory;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.MemoryRange;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.write.FrameSort;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.frame.write.FrameWriterUtils;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.List;

public class ColumnarFrameWriter implements FrameWriter
{
  private final RowSignature signature;
  private final List<ClusterByColumn> sortColumns;
  @Nullable // Null if frame will not need permutation
  private final AppendableMemory rowOrderMemory;
  private final List<FrameColumnWriter> columnWriters;
  private int numRows = 0;
  private boolean written = false;

  public ColumnarFrameWriter(
      final RowSignature signature,
      final List<ClusterByColumn> sortColumns,
      @Nullable final AppendableMemory rowOrderMemory,
      final List<FrameColumnWriter> columnWriters
  )
  {
    this.signature = signature;
    this.sortColumns = sortColumns;
    this.rowOrderMemory = rowOrderMemory;
    this.columnWriters = columnWriters;
  }

  @Override
  public boolean addSelection()
  {
    if (written) {
      throw new ISE("Cannot modify after writing");
    }

    if (numRows == Integer.MAX_VALUE) {
      return false;
    }

    if (rowOrderMemory != null && !rowOrderMemory.reserve(Integer.BYTES)) {
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
      if (rowOrderMemory != null) {
        final MemoryRange<WritableMemory> rowOrderCursor = rowOrderMemory.cursor();
        rowOrderCursor.memory().putInt(rowOrderCursor.start(), numRows);
        rowOrderMemory.advanceCursor(Integer.BYTES);
      }

      numRows++;
      return true;
    }
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }

  @Override
  public long getTotalSize()
  {
    return Frame.HEADER_SIZE + computeRowOrderSize() + computeRegionMapSize() + computeDataSize();
  }

  @Override
  public long writeTo(final WritableMemory memory, final long startPosition)
  {
    if (written) {
      throw new ISE("Cannot write twice");
    }

    final long totalSize = getTotalSize();
    long currentPosition = startPosition;

    currentPosition += FrameWriterUtils.writeFrameHeader(
        memory,
        startPosition,
        FrameType.COLUMNAR,
        totalSize,
        numRows,
        columnWriters.size(),
        mustSort()
    );

    if (mustSort()) {
      currentPosition += rowOrderMemory.writeTo(memory, currentPosition);
    }

    // Write region (column) ending positions.
    long columnEndPosition = Frame.HEADER_SIZE + computeRowOrderSize() + computeRegionMapSize();

    for (FrameColumnWriter writer : columnWriters) {
      columnEndPosition += writer.size();
      memory.putLong(currentPosition, columnEndPosition);
      currentPosition += Long.BYTES;
    }

    // Write regions (columns).
    for (FrameColumnWriter writer : columnWriters) {
      currentPosition += writer.writeTo(memory, currentPosition);
    }

    // Sanity check.
    if (currentPosition != totalSize) {
      throw new ISE("Expected to write [%,d] bytes, but wrote [%,d] bytes.", totalSize, currentPosition);
    }

    if (mustSort()) {
      FrameSort.sort(Frame.wrap(memory), FrameReader.create(signature), sortColumns);
    }

    written = true;
    return totalSize;
  }

  @Override
  public void close()
  {
    if (rowOrderMemory != null) {
      rowOrderMemory.close();
    }

    columnWriters.forEach(FrameColumnWriter::close);
  }

  /**
   * Computes the size of the row-order-permutation section of the frame.
   */
  private long computeRowOrderSize()
  {
    return mustSort() ? rowOrderMemory.size() : 0;
  }

  /**
   * Computes the size of the region-map section of the frame. It has an ending offset for each region, and since
   * we have one column per region, that's one long for every entry in {@link #columnWriters}.
   */
  private long computeRegionMapSize()
  {
    return (long) columnWriters.size() * Long.BYTES;
  }

  /**
   * Computes the size of the data section of the frame, which contains all of the columnar data.
   */
  private long computeDataSize()
  {
    return columnWriters.stream().mapToLong(FrameColumnWriter::size).sum();
  }

  /**
   * Returns whether this writer must sort during {@link #writeTo}.
   */
  private boolean mustSort()
  {
    return rowOrderMemory != null;
  }
}
