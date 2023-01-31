/*
 * Copyright (c) 2021 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information of Imply Data, Inc.
 */

package io.imply.druid.sql.calcite.external.exception;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

/**
 * Wraps an error in an "error" field.
 */
@JsonDeserialize(builder = ErrorWrapper.Builder.class)
public final class ErrorWrapper
{
  @NotNull
  @Valid
  private final ErrorModel error;

  private ErrorWrapper(@NotNull @Valid final ErrorModel error)
  {
    this.error = error;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  /**
   * Build an error wrapper around an error.
   *
   * @param error The error to wrap
   * @return The immutable error wrapper to use in a response
   */
  public static ErrorWrapper withError(@NotNull @Valid final ErrorModel error)
  {
    final Builder builder = new Builder();
    return builder.error(error).build();
  }

  @JsonProperty
  public ErrorModel error()
  {
    return error;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ErrorWrapper errorWrapper = (ErrorWrapper) o;

    return Objects.equals(this.error, errorWrapper.error);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(error);
  }

  @Override
  public String toString()
  {
    return error.toString();
  }

  /**
   * Builds an immutable error wrapper.
   */
  @JsonPOJOBuilder
  public static final class Builder
  {
    private ErrorModel error;

    @JsonProperty
    public Builder error(final @NotNull ErrorModel error)
    {
      this.error = error;
      return this;
    }

    public ErrorWrapper build()
    {
      return new ErrorWrapper(error);
    }
  }
}
