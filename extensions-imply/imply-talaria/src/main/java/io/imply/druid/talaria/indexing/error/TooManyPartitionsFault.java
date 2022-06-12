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

import java.util.Objects;

@JsonTypeName(TooManyPartitionsFault.CODE)
public class TooManyPartitionsFault extends BaseTalariaFault
{
  static final String CODE = "TooManyPartitions";

  private final int maxPartitions;

  @JsonCreator
  public TooManyPartitionsFault(@JsonProperty("maxPartitions") final int maxPartitions)
  {
    super(
        CODE,
        "Too many partitions (max = %d); try breaking your query up into smaller queries or "
        + "using a larger target size",
        maxPartitions
    );
    this.maxPartitions = maxPartitions;
  }

  @JsonProperty
  public int getMaxPartitions()
  {
    return maxPartitions;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TooManyPartitionsFault that = (TooManyPartitionsFault) o;
    return maxPartitions == that.maxPartitions;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), maxPartitions);
  }
}
