/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@JsonDeserialize(builder = InlineAvroParseSchemaProvider.Builder.class)
public class InlineAvroParseSchemaProvider extends ParseSchemaProvider
{

  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid Map<String, Object> schema;

  public InlineAvroParseSchemaProvider(
      final @NotNull ParseSchemaProviderType type,
      final Map<String, Object> schema
  )
  {
    super(type);
    this.schema =
        ImmutableMap.copyOf(schema);
  }

  public static InlineAvroParseSchemaProvider.Builder builder()
  {
    return new InlineAvroParseSchemaProvider.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public InlineAvroParseSchemaProvider.Builder with()
  {
    return (new Builder())
        .type(this.getType())
        .schema(this.getSchema());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public InlineAvroParseSchemaProvider with(final java.util.function.Consumer<Builder> consumer)
  {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public InlineAvroParseSchemaProvider cloneWithReadOnlyDefaults()
  {
    return (new Builder())
        .schema(this.getSchema())
        .type(this.getType())
        .build();
  }


  @JsonProperty("schema")
  public Map<String, Object> getSchema()
  {
    return schema;
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
    InlineAvroParseSchemaProvider inlineAvroParseSchemaProvider = (InlineAvroParseSchemaProvider) o;
    return Objects.equals(this.schema, inlineAvroParseSchemaProvider.schema) &&
           super.equals(o);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(schema, super.hashCode());
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("class InlineAvroParseSchemaProvider {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
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

  @Override
  public void accept(final ParseSchemaProvider.Visitor visitor)
  {
    visitor.visit(this);
  }


  @JsonPOJOBuilder
  public static class Builder implements ParseSchemaProvider.IBuilder<InlineAvroParseSchemaProvider>
  {
    private @Valid ParseSchemaProviderType type;
    private @Valid Map<String, Object> schema = new HashMap<>();

    /**
     * Set type and return the builder.
     */
    @Override
    @JsonProperty("type")
    public Builder type(
        final @Valid @NotNull
        ParseSchemaProviderType type
    )
    {
      this.type = type;
      return this;
    }


    /**
     * Set schema and return the builder.
     * An Avro reader schema. For example: &#x60;&#x60;&#x60; {      \&quot;type\&quot;: \&quot;record\&quot;,      \&quot;namespace\&quot;: \&quot;com.example\&quot;,      \&quot;name\&quot;: \&quot;FullName\&quot;,      \&quot;fields\&quot;: [        { \&quot;name\&quot;: \&quot;first\&quot;, \&quot;type\&quot;: \&quot;string\&quot; },        { \&quot;name\&quot;: \&quot;last\&quot;, \&quot;type\&quot;: \&quot;string\&quot; }      ] }
     */
    @JsonProperty("schema")
    public Builder schema(final @Valid Map<String, Object> schema)
    {
      this.schema = new LinkedHashMap<>(schema);
      return this;
    }


    public Builder putSchemaItem(final String key, final Object schemaItem)
    {
      if (this.schema == null) {
        this.schema = new HashMap<>();
      }

      this.schema.put(key, schemaItem);
      return this;
    }

    public Builder removeSchemaItem(final String key)
    {
      if (this.schema != null) {
        this.schema.remove(key);
      }

      return this;
    }

    @Override
    public InlineAvroParseSchemaProvider build()
    {
      return new InlineAvroParseSchemaProvider(type, schema);
    }
  }
}
