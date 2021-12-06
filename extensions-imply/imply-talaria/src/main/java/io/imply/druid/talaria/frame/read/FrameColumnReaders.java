/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read;

import org.apache.druid.segment.column.ColumnType;

public class FrameColumnReaders
{
  private FrameColumnReaders()
  {
    // No instantiation.
  }

  public static FrameColumnReader create(final int columnNumber, final ColumnType columnType)
  {
    switch (columnType.getType()) {
      case LONG:
        return new LongFrameColumnReader(columnNumber);

      case FLOAT:
        return new FloatFrameColumnReader(columnNumber);

      case DOUBLE:
        return new DoubleFrameColumnReader(columnNumber);

      case STRING:
        return new StringFrameColumnReader(columnNumber);

      default:
        return new ComplexFrameColumnReader(columnNumber);
    }
  }
}
