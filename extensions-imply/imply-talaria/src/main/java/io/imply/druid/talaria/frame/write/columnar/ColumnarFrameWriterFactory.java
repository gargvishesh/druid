/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write.columnar;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.AppendableMemory;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.frame.write.FrameWriterFactory;
import io.imply.druid.talaria.frame.write.FrameWriterUtils;
import io.imply.druid.talaria.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.utils.CloseableUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ColumnarFrameWriterFactory implements FrameWriterFactory
{
  private final MemoryAllocator allocator;
  private final RowSignature signature;
  private final List<ClusterByColumn> sortColumns;

  /**
   * Create a ColumnarFrameWriterFactory.
   *
   * @param allocator   memory allocator; will use as much as possible
   * @param signature   output signature for this frame writer
   * @param sortColumns columns to sort by, if any. May be empty.
   *
   * @throws UnsupportedColumnTypeException if "signature" contains any type that we cannot handle
   */
  public ColumnarFrameWriterFactory(
      final MemoryAllocator allocator,
      final RowSignature signature,
      final List<ClusterByColumn> sortColumns
  )
  {
    this.allocator = Preconditions.checkNotNull(allocator, "allocator");
    this.signature = signature;
    this.sortColumns = Preconditions.checkNotNull(sortColumns, "sortColumns");

    if (!FrameWriterUtils.areSortColumnsPrefixOfSignature(signature, sortColumns)) {
      throw new IAE("Sort columns must be a prefix of the signature");
    }

    // Check for disallowed field names.
    final Set<String> disallowedFieldNames = FrameWriterUtils.findDisallowedFieldNames(signature);
    if (!disallowedFieldNames.isEmpty()) {
      throw new IAE("Disallowed field names: %s", disallowedFieldNames);
    }
  }

  @Override
  public FrameWriter newFrameWriter(final ColumnSelectorFactory columnSelectorFactory)
  {
    final List<FrameColumnWriter> columnWriters = new ArrayList<>();

    try {
      for (int i = 0; i < signature.size(); i++) {
        final String column = signature.getColumnName(i);
        // note: null type won't work, but we'll get a nice error from FrameColumnWriters.create
        final ColumnType columnType = signature.getColumnType(i).orElse(null);
        columnWriters.add(FrameColumnWriters.create(columnSelectorFactory, allocator, column, columnType));
      }
    }
    catch (Throwable e) {
      // FrameColumnWriters.create can throw exceptions. If this happens, we need to close previously-created writers.
      throw CloseableUtils.closeAndWrapInCatch(e, () -> CloseableUtils.closeAll(columnWriters));
    }

    // Only need rowOrderMemory if we are sorting.
    final AppendableMemory rowOrderMemory = sortColumns.isEmpty() ? null : AppendableMemory.create(allocator);

    return new ColumnarFrameWriter(
        signature,
        sortColumns,
        rowOrderMemory,
        columnWriters
    );
  }

  @Override
  public long allocatorCapacity()
  {
    return allocator.capacity();
  }
}
