/*
 * Copyright (c) 2021 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information of Imply Data, Inc.
 */

package io.imply.druid.sql.calcite.external.exception;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;

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
@JsonDeserialize(builder = InnerError.Builder.class)
public class InnerError
{
  private final @NotNull String code;

  private final String message;

  private final Map<String, Object> fields;

  public InnerError(final @NotNull String code, final String message)
  {
    this.code = code;
    this.message = message;
    this.fields = new HashMap<>();
  }

  public InnerError(final @NotNull String code, final String message, final Map<String, Object> fields)
  {
    this.code = code;
    this.message = message;
    this.fields = ImmutableMap.copyOf(fields);
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonProperty
  public String code()
  {
    return code;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public String message()
  {
    return message;
  }

  @JsonAnyGetter
  public Map<String, Object> fields()
  {
    return fields;
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
    final InnerError innerError = (InnerError) o;

    return Objects.equals(this.code, innerError.code)
           && Objects.equals(this.message, innerError.message)
           && Objects.equals(this.fields, innerError.fields);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(code, message, fields);
  }

  /**
   * Builder for inner error.
   */
  public static class Builder
  {
    private String code;
    private String message;
    private final Map<String, Object> fields;

    public Builder()
    {
      this.code = "Unknown";
      this.message = "";
      this.fields = new HashMap<>();
    }

    @JsonProperty
    public Builder code(final String code)
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
    @JsonAnySetter
    public Builder field(final String key, final Object value)
    {
      // don't allow null key or value as will do Map.copyOf.
      if (key == null || value == null) {
        return this;
      }

      this.fields.put(key, value);
      return this;
    }

    public InnerError build()
    {
      return new InnerError(code, message, fields);
    }
  }
}
