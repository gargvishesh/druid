/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

public class FieldReaders
{
  private FieldReaders()
  {
    // No instantiation.
  }

  /**
   * Helper used by {@link io.imply.druid.talaria.frame.read.FrameReader}.
   */
  public static FieldReader create(final ColumnType columnType)
  {
    switch (Preconditions.checkNotNull(columnType, "columnType").getType()) {
      case LONG:
        return new LongFieldReader();

      case FLOAT:
        return new FloatFieldReader();

      case DOUBLE:
        return new DoubleFieldReader();

      case STRING:
        return new StringFieldReader(false);

      case COMPLEX:
        return ComplexFieldReader.createFromType(columnType);

      case ARRAY:
        if (columnType.getElementType().getType() == ValueType.STRING) {
          return new StringFieldReader(true);
        }
        // Fall through to error for other array types

      default:
        throw new UOE("Unsupported column type [%s]", columnType);
    }
  }
}
