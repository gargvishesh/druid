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

@JsonTypeName(TooManyColumnsFault.CODE)
public class TooManyColumnsFault extends BaseTalariaFault
{
  static final String CODE = "TooManyColumns";

  private final int numColumns;
  private final int maxColumns;

  @JsonCreator
  public TooManyColumnsFault(
      @JsonProperty("numColumns") final int numColumns,
      @JsonProperty("maxColumns") final int maxColumns
  )
  {
    super(CODE, "Too many output columns (requested = %d, max = %d)", numColumns, maxColumns);
    this.numColumns = numColumns;
    this.maxColumns = maxColumns;
  }

  @JsonProperty
  public int getNumColumns()
  {
    return numColumns;
  }

  @JsonProperty
  public int getMaxColumns()
  {
    return maxColumns;
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
    TooManyColumnsFault that = (TooManyColumnsFault) o;
    return numColumns == that.numColumns && maxColumns == that.maxColumns;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), numColumns, maxColumns);
  }
}
