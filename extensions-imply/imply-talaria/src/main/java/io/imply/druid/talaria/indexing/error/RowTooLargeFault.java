/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

// TODO(gianm): use this somewhere
@JsonTypeName(RowTooLargeFault.CODE)
public class RowTooLargeFault extends BaseTalariaFault
{
  static final String CODE = "RowTooLarge";

  private final long maxFrameSize;

  @JsonCreator
  public RowTooLargeFault(@JsonProperty("maxFrameSize") final long maxFrameSize)
  {
    super(CODE, "Encountered row that cannot fit in a single frame (max frame size = %,d)", maxFrameSize);
    this.maxFrameSize = maxFrameSize;
  }

  @JsonProperty
  public long getMaxFrameSize()
  {
    return maxFrameSize;
  }
}
