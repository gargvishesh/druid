/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

import io.imply.druid.talaria.frame.write.FrameWriterUtils;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.segment.ColumnValueSelector;

import java.util.List;

/**
 * Like {@link StringFieldWriter}, but reads arrays from a {@link ColumnValueSelector} instead of reading from
 * a {@link org.apache.druid.segment.DimensionSelector}.
 */
public class StringArrayFieldWriter implements FieldWriter
{
  private final ColumnValueSelector<List<String>> selector;

  public StringArrayFieldWriter(final ColumnValueSelector<List<String>> selector)
  {
    this.selector = selector;
  }

  @Override
  public long writeTo(WritableMemory memory, long position, long maxSize)
  {
    return StringFieldWriter.writeUtf8ByteBuffers(
        memory,
        position,
        maxSize,
        FrameWriterUtils.getUtf8ByteBuffersFromStringArraySelector(selector)
    );
  }

  @Override
  public void close()
  {
    // Nothing to do.
  }
}
