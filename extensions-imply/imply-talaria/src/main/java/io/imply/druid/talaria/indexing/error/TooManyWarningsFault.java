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

@JsonTypeName(TooManyWarningsFault.CODE)
public class TooManyWarningsFault extends BaseTalariaFault
{
  static final String CODE = "TooManyWarnings";

  private final int maxWarnings;
  private final String errorCode;

  @JsonCreator
  public TooManyWarningsFault(
      @JsonProperty("maxWarnings") final int maxWarnings,
      @JsonProperty("errorCode") final String errorCode
  )
  {
    super(CODE, "Too many warnings of type %s generated (max = %d)", errorCode, maxWarnings);
    this.maxWarnings = maxWarnings;
    this.errorCode = errorCode;
  }

  @JsonProperty
  public int getMaxWarnings()
  {
    return maxWarnings;
  }

  @Override
  @JsonProperty
  public String getErrorCode()
  {
    return errorCode;
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
    TooManyWarningsFault that = (TooManyWarningsFault) o;
    return maxWarnings == that.maxWarnings && Objects.equals(errorCode, that.errorCode);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), maxWarnings, errorCode);
  }
}
