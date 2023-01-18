package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableMap;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@JsonDeserialize(builder = AvroOcfFormatSettings.Builder.class)
public class AvroOcfFormatSettings extends DataFormatSettings {

  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid Boolean binaryAsString;
  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid Boolean extractUnionsByType;
  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid Map<String, Object> schema;

  public AvroOcfFormatSettings(
      final @NotNull DataFormat format,
      final Boolean binaryAsString,
      final Boolean extractUnionsByType,
      final Map<String, Object> schema
  ) {
    super(format);
    this.binaryAsString = binaryAsString;
    this.extractUnionsByType = extractUnionsByType;
    this.schema =
        ImmutableMap.copyOf(schema);
  }

  public static AvroOcfFormatSettings.Builder builder() {
    return new AvroOcfFormatSettings.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public AvroOcfFormatSettings.Builder with() {
    return (new Builder())
        .format(this.getFormat())
        .binaryAsString(this.getBinaryAsString())
        .extractUnionsByType(this.getExtractUnionsByType())
        .schema(this.getSchema());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public AvroOcfFormatSettings with(final java.util.function.Consumer<Builder> consumer) {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public AvroOcfFormatSettings cloneWithReadOnlyDefaults() {
    return (new Builder())
        .binaryAsString(this.getBinaryAsString())
        .extractUnionsByType(this.getExtractUnionsByType())
        .schema(this.getSchema())
        .format(this.getFormat())
        .build();
  }




  @JsonProperty("binaryAsString")
  public Boolean getBinaryAsString() {
    return binaryAsString;
  }

  @JsonProperty("extractUnionsByType")
  public Boolean getExtractUnionsByType() {
    return extractUnionsByType;
  }

  @JsonProperty("schema")
  public Map<String, Object> getSchema() {
    return schema;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AvroOcfFormatSettings avroOcfFormatSettings = (AvroOcfFormatSettings) o;
    return Objects.equals(this.binaryAsString, avroOcfFormatSettings.binaryAsString) &&
           Objects.equals(this.extractUnionsByType, avroOcfFormatSettings.extractUnionsByType) &&
           Objects.equals(this.schema, avroOcfFormatSettings.schema) &&
           super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(binaryAsString, extractUnionsByType, schema, super.hashCode());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AvroOcfFormatSettings {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    binaryAsString: ").append(toIndentedString(binaryAsString)).append("\n");
    sb.append("    extractUnionsByType: ").append(toIndentedString(extractUnionsByType)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(final Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

  @Override
  public void accept(final DataFormatSettings.Visitor visitor) {
    visitor.visit(this);
  }


  @JsonPOJOBuilder
  public static class Builder implements DataFormatSettings.IBuilder<AvroOcfFormatSettings> {
    private @Valid DataFormat format;
    private @Valid Boolean binaryAsString = false;
    private @Valid Boolean extractUnionsByType = false;
    private @Valid Map<String, Object> schema = new HashMap<>();

    /**
     * Set format and return the builder.
     */
    @Override
    @JsonProperty("format")
    public Builder format(final @Valid  @NotNull
                          DataFormat format) {
      this.format = format;
      return this;
    }


    /**
     * Set binaryAsString and return the builder.
     * Specifies if the bytes Avro column which is not logically marked as a string or enum type should be treated as a UTF-8 encoded string.
     */
    @JsonProperty("binaryAsString")
    public Builder binaryAsString(final @Valid Boolean binaryAsString) {
      this.binaryAsString = binaryAsString;
      return this;
    }


    /**
     * Set extractUnionsByType and return the builder.
     * If you want to operate on individual members of a union, set &#x60;extractUnionsByType&#x60; on the Avro parser. This configuration expands union values into nested objects according to the following rules: - Primitive types and unnamed complex types are keyed by their type name, such as &#x60;int&#x60; and &#x60;string&#x60;. - Complex named types are keyed by their names, this includes &#x60;record&#x60;, &#x60;fixed&#x60;, and &#x60;enum&#x60;. - The Avro null type is elided as its value can only ever be null.
     */
    @JsonProperty("extractUnionsByType")
    public Builder extractUnionsByType(final @Valid Boolean extractUnionsByType) {
      this.extractUnionsByType = extractUnionsByType;
      return this;
    }


    /**
     * Set schema and return the builder.
     * Define a reader schema to be used when parsing Avro records. This is useful if you want to override the reader schema included in the Avro OCF file data. For example: &#x60;&#x60;&#x60; {      \&quot;type\&quot;: \&quot;record\&quot;,      \&quot;namespace\&quot;: \&quot;com.example\&quot;,      \&quot;name\&quot;: \&quot;FullName\&quot;,      \&quot;fields\&quot;: [        { \&quot;name\&quot;: \&quot;first\&quot;, \&quot;type\&quot;: \&quot;string\&quot; },        { \&quot;name\&quot;: \&quot;last\&quot;, \&quot;type\&quot;: \&quot;string\&quot; }      ] } &#x60;&#x60;&#x60;
     */
    @JsonProperty("schema")
    public Builder schema(final @Valid Map<String, Object> schema) {
      this.schema = new LinkedHashMap<>(schema);
      return this;
    }


    public Builder putSchemaItem(final String key, final Object schemaItem) {
      if (this.schema == null) {
        this.schema = new HashMap<>();
      }

      this.schema.put(key, schemaItem);
      return this;
    }

    public Builder removeSchemaItem(final String key) {
      if (this.schema != null) {
        this.schema.remove(key);
      }

      return this;
    }

    @Override
    public AvroOcfFormatSettings build() {
      return new AvroOcfFormatSettings(format, binaryAsString, extractUnionsByType, schema);
    }
  }
}
