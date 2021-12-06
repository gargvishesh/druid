/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Objects;

public abstract class BaseTalariaFault implements TalariaFault
{
  private final String errorCode;

  @Nullable
  private final String errorMessage;

  BaseTalariaFault(final String errorCode, @Nullable final String errorMessage)
  {
    this.errorCode = Preconditions.checkNotNull(errorCode, "errorCode");
    this.errorMessage = errorMessage;
  }

  BaseTalariaFault(
      final String errorCode,
      final String errorMessageFormat,
      final Object errorMessageFirstArg,
      final Object... errorMessageOtherArgs
  )
  {
    this(errorCode, format(errorMessageFormat, errorMessageFirstArg, errorMessageOtherArgs));
  }

  BaseTalariaFault(final String errorCode)
  {
    this(errorCode, null);
  }

  @Override
  @JsonProperty
  public String getErrorCode()
  {
    return errorCode;
  }

  @Override
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getErrorMessage()
  {
    return errorMessage;
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
    BaseTalariaFault that = (BaseTalariaFault) o;
    return Objects.equals(errorCode, that.errorCode) && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(errorCode, errorMessage);
  }

  @Override
  public String toString()
  {
    return getCodeWithMessage();
  }

  private static String format(
      final String formatString,
      final Object firstArg,
      final Object... otherArgs
  )
  {
    final Object[] args = new Object[1 + (otherArgs != null ? otherArgs.length : 0)];

    args[0] = firstArg;

    if (otherArgs != null) {
      System.arraycopy(otherArgs, 0, args, 1, otherArgs.length);
    }

    return StringUtils.format(formatString, args);
  }
}
