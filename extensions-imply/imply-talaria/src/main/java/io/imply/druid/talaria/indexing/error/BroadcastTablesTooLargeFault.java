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

@JsonTypeName(BroadcastTablesTooLargeFault.CODE)
public class BroadcastTablesTooLargeFault extends BaseTalariaFault
{
  static final String CODE = "BroadcastTablesTooLarge";

  private final long maxBroadcastTablesSize;

  @JsonCreator
  public BroadcastTablesTooLargeFault(@JsonProperty("maxBroadcastTablesSize") final long maxBroadcastTablesSize)
  {
    super(CODE,
          "Size of the broadcast tables exceed the memory reserved for them (memory reserved for broadcast tables = %d bytes)",
          maxBroadcastTablesSize
    );
    this.maxBroadcastTablesSize = maxBroadcastTablesSize;
  }

  @JsonProperty
  public long getMaxBroadcastTablesSize()
  {
    return maxBroadcastTablesSize;
  }
}
