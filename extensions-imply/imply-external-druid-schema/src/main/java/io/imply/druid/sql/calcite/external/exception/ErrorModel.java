/*
 * Copyright (c) 2021 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information of Imply Data, Inc.
 */

package io.imply.druid.sql.calcite.external.exception;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Represents a high-level error.
 */
@JsonDeserialize(builder = ErrorModel.Builder.class)
public final class ErrorModel
{
  @NotNull
  private final ImplyErrorCode code;

  @NotNull
  private final String message;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String target;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final List<ErrorModel> details;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final InnerError innerError;

  private ErrorModel(
      final ImplyErrorCode code,
      final String message,
      final String target,
      final List<ErrorModel> details,
      final InnerError innerError
  )
  {
    this.code = code;
    this.message = message;
    this.target = target;
    this.details = (details != null ? ImmutableList.copyOf(details) : Collections.emptyList());
    this.innerError = innerError;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonProperty
  public ImplyErrorCode code()
  {
    return code;
  }

  @JsonProperty
  public String message()
  {
    return message;
  }

  @JsonProperty
  public String target()
  {
    return target;
  }

  @JsonProperty
  public List<ErrorModel> details()
  {
    return details;
  }

  @JsonProperty("innererror")
  public InnerError innerError()
  {
    return innerError;
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
    final ErrorModel error = (ErrorModel) o;

    return Objects.equals(this.code, error.code)
           && Objects.equals(this.message, error.message)
           && Objects.equals(this.target, error.target)
           && Objects.equals(this.details, error.details)
           && Objects.equals(this.innerError, error.innerError);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(code, message, target, details, innerError);
  }

  @Override
  public String toString()
  {
    final StringJoiner joiner = new StringJoiner(":", "imply.Error:", "");
    joiner.add(code.value());

    if (target != null) {
      joiner.add(target);
    }

    if (message != null) {
      joiner.add(" " + message);
    }

    return joiner.toString();
  }

  /**
   * Builds an immutable error object.
   */
  @JsonPOJOBuilder
  public static final class Builder
  {
    private ImplyErrorCode code;
    private String message;
    private String target;
    private List<ErrorModel> details;
    private InnerError innerError;

    @JsonProperty
    public Builder code(final @NotNull ImplyErrorCode code)
    {
      this.code = code;
      return this;
    }

    @JsonProperty
    public Builder message(final String message)
    {
      this.message = message;
      return this;
    }

    @JsonProperty
    public Builder target(final String target)
    {
      this.target = target;
      return this;
    }

    @JsonProperty
    public Builder details(final @NotNull List<ErrorModel> details)
    {
      this.details = new ArrayList<>(details);
      return this;
    }

    public Builder addDetails(final @NotNull List<ErrorModel> details)
    {
      if (this.details == null) {
        this.details = new ArrayList<>();
      }
      this.details.addAll(details);
      return this;
    }

    public Builder addDetail(final @NotNull ErrorModel detail)
    {
      if (this.details == null) {
        this.details = new ArrayList<>();
      }
      this.details.add(detail);
      return this;
    }

    @JsonProperty("innererror")
    public Builder innerError(final InnerError innerError)
    {
      this.innerError = innerError;
      return this;
    }

    public ErrorModel build()
    {
      return new ErrorModel(code, message, target, details, innerError);
    }
  }
}
