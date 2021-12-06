/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

/**
 * Dimension-schema-related utility functions that would make sense in {@link DimensionSchema} if this
 * were not an extension.
 */
public class DimensionSchemaUtils
{
  public static DimensionSchema createDimensionSchema(final String column, @Nullable final ValueType type)
  {
    if (type == null || type == ValueType.STRING) {
      return new StringDimensionSchema(column);
    } else if (type == ValueType.LONG) {
      return new LongDimensionSchema(column);
    } else if (type == ValueType.FLOAT) {
      return new FloatDimensionSchema(column);
    } else if (type == ValueType.DOUBLE) {
      return new DoubleDimensionSchema(column);
    } else {
      throw new ISE("Cannot create dimension for type [%s]", type);
    }
  }
}
