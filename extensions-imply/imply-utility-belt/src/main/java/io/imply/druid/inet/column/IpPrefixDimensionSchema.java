/*
 *
 *  * Copyright (c) Imply Data, Inc. All rights reserved.
 *  *
 *  * This software is the confidential and proprietary information
 *  * of Imply Data, Inc. You shall not disclose such Confidential
 *  * Information and shall use it only in accordance with the terms
 *  * of the license agreement you entered into with Imply.
 *
 *
 */

package io.imply.druid.inet.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.inet.IpAddressModule;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.column.ColumnType;

public class IpPrefixDimensionSchema extends DimensionSchema
{
  @JsonCreator
  public IpPrefixDimensionSchema(
      @JsonProperty("name") String name,
      @JsonProperty("createBitmapIndex") Boolean createBitmapIndex
  )
  {
    super(name, null, createBitmapIndex == null ? true : createBitmapIndex.booleanValue());
  }

  @Override
  public String getTypeName()
  {
    return IpAddressModule.PREFIX_TYPE_NAME;
  }

  @Override
  public ColumnType getColumnType()
  {
    return IpAddressModule.PREFIX_TYPE;
  }
}
