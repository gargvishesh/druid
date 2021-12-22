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
import org.apache.druid.segment.data.BitmapSerdeFactory;

import java.nio.ByteOrder;

public class NestedDataColumnMetadata
{
  private final ByteOrder byteOrder;
  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final String fileNameBase;
  private final Boolean hasNulls;

  @JsonCreator
  public NestedDataColumnMetadata(
      @JsonProperty("byteOrder") ByteOrder byteOrder,
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("fileNameBase") String fileNameBase,
      @JsonProperty("hasNulls") Boolean hasNulls
  )
  {
    this.byteOrder = byteOrder;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.fileNameBase = fileNameBase;
    this.hasNulls = hasNulls;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty
  public String getFileNameBase()
  {
    return fileNameBase;
  }

  @JsonProperty("hasNulls")
  public Boolean hasNulls()
  {
    return hasNulls;
  }
}
