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
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

/**
 * Dimension-schema-related utility functions that would make sense in {@link DimensionSchema} if this
 * were not an extension.
 */
public class DimensionSchemaUtils
{
  public static DimensionSchema createDimensionSchema(final String column, @Nullable final ColumnType type)
  {
    // if schema information not available, create a string dimension
    if (type == null) {
      return new StringDimensionSchema(column);
    }

    switch (type.getType()) {
      case STRING:
        return new StringDimensionSchema(column);
      case LONG:
        return new LongDimensionSchema(column);
      case FLOAT:
        return new FloatDimensionSchema(column);
      case DOUBLE:
        return new DoubleDimensionSchema(column);
      case ARRAY:
        switch (type.getElementType().getType()) {
          case STRING:
            return new StringDimensionSchema(column, DimensionSchema.MultiValueHandling.ARRAY, null);
          default:
            throw new ISE("Cannot create dimension for type [%s]", type.toString());
        }
      default:
        final ColumnCapabilities capabilities = ColumnCapabilitiesImpl.createDefault().setType(type);
        return DimensionHandlerUtils.getHandlerFromCapabilities(column, capabilities, null)
                                    .getDimensionSchema(capabilities);
    }
  }
}
