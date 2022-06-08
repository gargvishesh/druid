/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import io.imply.druid.talaria.frame.AppendableMemory;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.field.FieldWriter;
import io.imply.druid.talaria.frame.field.FieldWriters;
import io.imply.druid.talaria.frame.read.FrameReaderUtils;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.utils.CloseableUtils;

import java.util.ArrayList;
import java.util.List;

public class RowBasedFrameWriterFactory implements FrameWriterFactory
{
  private final MemoryAllocator allocator;
  private final RowSignature signature;
  private final List<ClusterByColumn> sortColumns;

  public RowBasedFrameWriterFactory(
      final MemoryAllocator allocator,
      final RowSignature signature,
      final List<ClusterByColumn> sortColumns
  )
  {
    this.allocator = allocator;
    this.signature = signature;
    this.sortColumns = sortColumns;
  }

  @Override
  public FrameWriter newFrameWriter(final ColumnSelectorFactory columnSelectorFactory)
  {
    // Only need rowOrderMemory if we are sorting.
    final AppendableMemory rowOrderMemory = sortColumns.isEmpty() ? null : AppendableMemory.create(allocator);
    final AppendableMemory rowOffsetMemory = AppendableMemory.create(allocator);
    final AppendableMemory dataMemory = AppendableMemory.create(
        allocator,
        RowBasedFrameWriter.BASE_DATA_ALLOCATION_SIZE
    );

    return new RowBasedFrameWriter(
        signature,
        sortColumns,
        makeFieldWriters(columnSelectorFactory),
        FrameReaderUtils.makeRowMemorySupplier(columnSelectorFactory, signature),
        rowOrderMemory,
        rowOffsetMemory,
        dataMemory
    );
  }

  @Override
  public long allocatorCapacity()
  {
    return allocator.capacity();
  }

  /**
   * Returns field writers that source data from the provided {@link ColumnSelectorFactory}.
   *
   * The returned {@link FieldWriter} objects are not thread-safe, and should only be used with a
   * single frame writer.
   */
  private List<FieldWriter> makeFieldWriters(final ColumnSelectorFactory columnSelectorFactory)
  {
    final List<FieldWriter> fieldWriters = new ArrayList<>();

    try {
      for (int i = 0; i < signature.size(); i++) {
        final String column = signature.getColumnName(i);
        // note: null type won't work, but we'll get a nice error from FrameColumnWriters.create
        final ColumnType columnType = signature.getColumnType(i).orElse(null);
        fieldWriters.add(FieldWriters.create(columnSelectorFactory, column, columnType));
      }
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, () -> CloseableUtils.closeAll(fieldWriters));
    }

    return fieldWriters;
  }
}
