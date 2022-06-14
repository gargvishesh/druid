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

@JsonTypeName(InvalidNullByteFault.CODE)
public class InvalidNullByteFault extends BaseTalariaFault
{
  static final String CODE = "InvalidNullByte";

  private final String column;

  @JsonCreator
  public InvalidNullByteFault(
      @JsonProperty("column") final String column
  )
  {
    super(CODE, "Invalid null byte in string column [%s]", column);
    this.column = column;
  }

  @JsonProperty
  public String getColumn()
  {
    return column;
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
    InvalidNullByteFault that = (InvalidNullByteFault) o;
    return Objects.equals(column, that.column);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), column);
  }
}
