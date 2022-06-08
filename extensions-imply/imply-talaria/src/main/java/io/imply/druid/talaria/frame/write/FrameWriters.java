/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.write.columnar.ColumnarFrameWriterFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;

/**
 * Holds a static method for making {@link FrameWriterFactory} instances.
 */
public class FrameWriters
{
  private FrameWriters()
  {
    // No instantiation.
  }

  /**
   * Creates a {@link FrameWriterFactory} that produces frames of the given {@link FrameType}.
   */
  public static FrameWriterFactory makeFrameWriterFactory(
      final FrameType frameType,
      final MemoryAllocator allocator,
      final RowSignature signature,
      final List<ClusterByColumn> sortColumns
  )
  {
    switch (Preconditions.checkNotNull(frameType, "frameType")) {
      case COLUMNAR:
        return new ColumnarFrameWriterFactory(allocator, signature, sortColumns);
      case ROW_BASED:
        return new RowBasedFrameWriterFactory(allocator, signature, sortColumns);
      default:
        throw new ISE("Unrecognized frame type [%s]", frameType);
    }
  }
}
