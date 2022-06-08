/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.HeapMemoryAllocator;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.frame.write.FrameWriterFactory;
import io.imply.druid.talaria.frame.write.FrameWriters;
import io.imply.druid.talaria.frame.write.RowBasedFrameWriter;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ClusterByTestUtils
{
  private ClusterByTestUtils()
  {
    // No instantiation.
  }

  /**
   * Create a signature matching {@code sortColumns}, using types from {@code inspector}.
   */
  public static RowSignature createKeySignature(
      final List<ClusterByColumn> sortColumns,
      final ColumnInspector inspector
  )
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (final ClusterByColumn sortColumn : sortColumns) {
      final ColumnCapabilities capabilities = inspector.getColumnCapabilities(sortColumn.columnName());
      final ColumnType columnType =
          Optional.ofNullable(capabilities).map(ColumnCapabilities::toColumnType).orElse(null);
      builder.add(sortColumn.columnName(), columnType);
    }

    return builder.build();
  }

  /**
   * Create a {@link ClusterByKey}.
   *
   * @param keySignature signature to use for the keys
   * @param objects      key field values
   */
  public static ClusterByKey createKey(
      final RowSignature keySignature,
      final Object... objects
  )
  {
    final RowBasedColumnSelectorFactory<Object[]> columnSelectorFactory = RowBasedColumnSelectorFactory.create(
        columnName -> {
          final int idx = keySignature.indexOf(columnName);

          if (idx < 0) {
            return arr -> null;
          } else {
            return arr -> arr[idx];
          }
        },
        () -> objects,
        keySignature,
        true,
        false
    );

    final FrameWriterFactory writerFactory = FrameWriters.makeFrameWriterFactory(
        FrameType.ROW_BASED,
        HeapMemoryAllocator.unlimited(),
        keySignature,
        Collections.emptyList()
    );

    try (final FrameWriter writer = writerFactory.newFrameWriter(columnSelectorFactory)) {
      writer.addSelection();
      final Frame frame = Frame.wrap(writer.toByteArray());
      final Memory dataRegion = frame.region(RowBasedFrameWriter.ROW_DATA_REGION);
      final byte[] keyBytes = new byte[(int) dataRegion.getCapacity()];
      dataRegion.copyTo(0, WritableMemory.writableWrap(keyBytes), 0, keyBytes.length);
      return ClusterByKey.wrap(keyBytes);
    }
  }
}
