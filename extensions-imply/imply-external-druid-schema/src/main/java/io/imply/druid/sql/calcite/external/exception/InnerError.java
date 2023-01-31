/*
 * Copyright (c) 2021 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information of Imply Data, Inc.
 */

package io.imply.druid.sql.calcite.external.exception;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A detailed, service-specific error.
 * <p>
 * In contrast to {@link ErrorModel}, this object has a `code` that is a free-form string and MAY be a code that is
 * specific to a domain or service. It SHOULD still contain a human-readable, ASCII English, searchable error code
 * like `TableNotFound` or `QuerySyntaxError`.
 * <p>
 * Extend this class and add fields annotated with `@JsonProperty` to add fields that detail the error. For example,
 * a hypothetical `InsecurePassword` error could have `minLength`, `requiredCharacterClasses`, etc. fields that,
 * if annotated as `@JsonProperty`, will be serialized into the error response.
 */
public class InnerError
{
  @JsonProperty
  private @NotNull String code;


  @JsonProperty
  private String message;

  private final Map<String, Object> fields = new HashMap<>();

  @JsonAnySetter
  public void add(String key, Object value)
  {
    fields.put(key, value);
  }

  @NotNull
  public String getCode()
  {
    return code;
  }

  public String getMessage()
  {
    return message;
  }

  public Map<String, Object> getFields()
  {
    return fields;
  }


  @Override
  public String toString()
  {
    return "InnerError{" + "code='" + code + '\'' + ", message='" + message + '\'' + ", fields=" + fields + '}';
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(code, message, fields);
  }
}
