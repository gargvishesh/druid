/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.data.BitmapSerdeFactory;

public class IpAddressBlobColumnMetadata
{
  private final BitmapSerdeFactory bitmapSerdeFactory;

  @JsonCreator
  public IpAddressBlobColumnMetadata(
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    this.bitmapSerdeFactory = bitmapSerdeFactory;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }
}
