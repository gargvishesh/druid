/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.column.ColumnType;

public class NestedDataDimensionSchema extends DimensionSchema
{
  @JsonCreator
  public NestedDataDimensionSchema(
      @JsonProperty("name") String name
  )
  {
    super(name, null, true);
  }

  @Override
  public String getTypeName()
  {
    return NestedDataComplexTypeSerde.TYPE_NAME;
  }

  @Override
  public ColumnType getColumnType()
  {
    return NestedDataComplexTypeSerde.TYPE;
  }
}

