/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

import io.imply.druid.talaria.frame.segment.row.ReadableFrameRowPointer;
import org.apache.datasketches.memory.Memory;

/**
 * A {@link ReadableFieldPointer} that is derived from a row-based frame.
 */
public class RowMemoryFieldPointer implements ReadableFieldPointer
{
  private final Memory memory;
  private final ReadableFrameRowPointer rowPointer;
  private final int fieldNumber;
  private final int fieldCount;

  public RowMemoryFieldPointer(
      final Memory memory,
      final ReadableFrameRowPointer rowPointer,
      final int fieldNumber,
      final int fieldCount
  )
  {
    this.memory = memory;
    this.rowPointer = rowPointer;
    this.fieldNumber = fieldNumber;
    this.fieldCount = fieldCount;
  }

  @Override
  public long position()
  {
    if (fieldNumber == 0) {
      return rowPointer.position() + (long) Integer.BYTES * fieldCount;
    } else {
      return rowPointer.position() + memory.getInt(rowPointer.position() + (long) Integer.BYTES * (fieldNumber - 1));
    }
  }
}
