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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName(UnknownFault.CODE)
public class UnknownFault extends BaseTalariaFault
{
  static final String CODE = "UnknownError";

  @Nullable
  private final String message;

  private UnknownFault(@Nullable final String message)
  {
    super(CODE, message);
    this.message = message;
  }

  @JsonCreator
  public static UnknownFault forMessage(@JsonProperty("message") @Nullable final String message)
  {
    return new UnknownFault(message);
  }

  public static UnknownFault forException(@Nullable final Throwable t)
  {
    return new UnknownFault(t == null ? null : t.toString());
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public String getMessage()
  {
    return message;
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
    UnknownFault that = (UnknownFault) o;
    return Objects.equals(message, that.message);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), message);
  }
}
