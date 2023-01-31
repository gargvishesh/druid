/*
 * Copyright (c) 2021 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information of Imply Data, Inc.
 */

package io.imply.druid.sql.calcite.external.exception;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * Represents a high-level error.
 */
@JsonRootName("error")
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
      @JsonProperty("code") final ImplyErrorCode code,
      @JsonProperty("message") final String message,
      @JsonProperty("target") final String target,
      @JsonProperty("details") final List<ErrorModel> details,
      @JsonProperty("innererror") final InnerError innerError
  )
  {
    this.code = code;
    this.message = message;
    this.target = target;
    this.details = (details != null ? ImmutableList.copyOf(details) : Collections.emptyList());
    this.innerError = innerError;
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

    if (innerError != null) {
      joiner.add("" + innerError);
    }

    return joiner.toString();
  }

  @NotNull
  public ImplyErrorCode getCode()
  {
    return code;
  }

  @NotNull
  public String getMessage()
  {
    return message;
  }

  public String getTarget()
  {
    return target;
  }

  public List<ErrorModel> getDetails()
  {
    return details;
  }

  public InnerError getInnerError()
  {
    return innerError;
  }
}
