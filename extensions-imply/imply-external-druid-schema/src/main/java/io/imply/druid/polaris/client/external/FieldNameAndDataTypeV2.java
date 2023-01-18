/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.druid.java.util.common.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

/**
 * A field name and its associated data type.
 */
public class FieldNameAndDataTypeV2
{


  /**
   * Data type of the input field.
   */
  public enum DataTypeEnum
  {
    DOUBLE("double"),

    FLOAT("float"),

    LONG("long"),

    STRING("string"),

    JSON("json");

    private String value;

    public String value()
    {
      return value;
    }

    DataTypeEnum(String value)
    {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString()
    {
      return String.valueOf(value);
    }

    @JsonCreator
    public static DataTypeEnum fromValue(String value)
    {
      for (DataTypeEnum b : DataTypeEnum.values()) {
        if (String.valueOf(b.value).replace('-', '_').equalsIgnoreCase(String.valueOf(value).replace('-', '_'))) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }


  private final @NotNull @Valid DataTypeEnum dataType;

  private final @NotNull @Valid String name;

  public FieldNameAndDataTypeV2(
      final DataTypeEnum dataType,
      final String name
  )
  {
    this.dataType = dataType;
    this.name = name;
  }

  public static FieldNameAndDataTypeV2.Builder builder()
  {
    return new FieldNameAndDataTypeV2.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public FieldNameAndDataTypeV2.Builder with()
  {
    return (new Builder())
        .dataType(this.getDataType())
        .name(this.getName());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public FieldNameAndDataTypeV2 with(final java.util.function.Consumer<Builder> consumer)
  {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public FieldNameAndDataTypeV2 cloneWithReadOnlyDefaults()
  {
    return (new Builder())
        .dataType(this.getDataType())
        .name(this.getName())
        .build();
  }


  @JsonProperty("dataType")
  @NotNull
  public DataTypeEnum getDataType()
  {
    return dataType;
  }

  @JsonProperty("name")
  @NotNull
  public String getName()
  {
    return name;
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
    FieldNameAndDataTypeV2 fieldNameAndDataTypeV2 = (FieldNameAndDataTypeV2) o;
    return Objects.equals(this.dataType, fieldNameAndDataTypeV2.dataType) &&
           Objects.equals(this.name, fieldNameAndDataTypeV2.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataType, name);
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("class FieldNameAndDataTypeV2 {\n");

    sb.append("    dataType: ").append(toIndentedString(dataType)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(final Object o)
  {
    if (o == null) {
      return "null";
    }
    return StringUtils.replaceChar(o.toString(), '\n', "\n    ");
  }


  @JsonPOJOBuilder
  public static class Builder
  {
    private @Valid DataTypeEnum dataType;
    private @Valid String name;

    /**
     * Set dataType and return the builder.
     * Data type of the input field.
     */
    @JsonProperty("dataType")
    public Builder dataType(
        final @Valid @NotNull
        DataTypeEnum dataType
    )
    {
      this.dataType = dataType;
      return this;
    }


    /**
     * Set name and return the builder.
     * Name of the input field.
     */
    @JsonProperty("name")
    public Builder name(
        final @Valid @NotNull
        String name
    )
    {
      this.name = name;
      return this;
    }


    public FieldNameAndDataTypeV2 build()
    {
      return new FieldNameAndDataTypeV2(dataType, name);
    }
  }
}
