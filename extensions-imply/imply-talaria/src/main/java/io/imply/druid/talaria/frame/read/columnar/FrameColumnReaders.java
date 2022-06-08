/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read.columnar;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

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
        return new StringFrameColumnReader(columnNumber, false);

      case COMPLEX:
        return new ComplexFrameColumnReader(columnNumber);

      case ARRAY:
        if (columnType.getElementType().getType() == ValueType.STRING) {
          return new StringFrameColumnReader(columnNumber, true);
        }
        // Fall through to error for other array types

      default:
        throw new UOE("Unsupported column type [%s]", columnType);
    }
  }
}
