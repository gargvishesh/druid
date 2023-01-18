package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@JsonDeserialize(builder = AvroStreamFormatSettings.Builder.class)
public class AvroStreamFormatSettings extends DataFormatSettings {

  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid ParseSchemaProvider parseSchemaProvider;
  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid Boolean binaryAsString;
  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid Boolean extractUnionsByType;

  public AvroStreamFormatSettings(
      final @NotNull DataFormat format,
      final ParseSchemaProvider parseSchemaProvider,
      final Boolean binaryAsString,
      final Boolean extractUnionsByType
  ) {
    super(format);
    this.parseSchemaProvider = parseSchemaProvider;
    this.binaryAsString = binaryAsString;
    this.extractUnionsByType = extractUnionsByType;
  }

  public static AvroStreamFormatSettings.Builder builder() {
    return new AvroStreamFormatSettings.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public AvroStreamFormatSettings.Builder with() {
    return (new Builder())
        .format(this.getFormat())
        .parseSchemaProvider(this.getParseSchemaProvider())
        .binaryAsString(this.getBinaryAsString())
        .extractUnionsByType(this.getExtractUnionsByType());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public AvroStreamFormatSettings with(final java.util.function.Consumer<Builder> consumer) {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public AvroStreamFormatSettings cloneWithReadOnlyDefaults() {
    return (new Builder())
        .parseSchemaProvider(this.getParseSchemaProvider())
        .binaryAsString(this.getBinaryAsString())
        .extractUnionsByType(this.getExtractUnionsByType())
        .format(this.getFormat())
        .build();
  }




  @JsonProperty("parseSchemaProvider")
  public ParseSchemaProvider getParseSchemaProvider() {
    return parseSchemaProvider;
  }

  @JsonProperty("binaryAsString")
  public Boolean getBinaryAsString() {
    return binaryAsString;
  }

  @JsonProperty("extractUnionsByType")
  public Boolean getExtractUnionsByType() {
    return extractUnionsByType;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AvroStreamFormatSettings avroStreamFormatSettings = (AvroStreamFormatSettings) o;
    return Objects.equals(this.parseSchemaProvider, avroStreamFormatSettings.parseSchemaProvider) &&
           Objects.equals(this.binaryAsString, avroStreamFormatSettings.binaryAsString) &&
           Objects.equals(this.extractUnionsByType, avroStreamFormatSettings.extractUnionsByType) &&
           super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parseSchemaProvider, binaryAsString, extractUnionsByType, super.hashCode());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AvroStreamFormatSettings {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    parseSchemaProvider: ").append(toIndentedString(parseSchemaProvider)).append("\n");
    sb.append("    binaryAsString: ").append(toIndentedString(binaryAsString)).append("\n");
    sb.append("    extractUnionsByType: ").append(toIndentedString(extractUnionsByType)).append("\n");
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
  public static class Builder implements DataFormatSettings.IBuilder<AvroStreamFormatSettings> {
    private @Valid DataFormat format;
    private @Valid ParseSchemaProvider parseSchemaProvider;
    private @Valid Boolean binaryAsString = false;
    private @Valid Boolean extractUnionsByType = false;

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
     * Set parseSchemaProvider and return the builder.
     */
    @JsonProperty("parseSchemaProvider")
    public Builder parseSchemaProvider(final @Valid ParseSchemaProvider parseSchemaProvider) {
      this.parseSchemaProvider = parseSchemaProvider;
      return this;
    }


    /**
     * Set binaryAsString and return the builder.
     * For a bytes Avro column that is not logically marked as a string or enum type, specifies if the column should be treated as a UTF-8 encoded string.
     */
    @JsonProperty("binaryAsString")
    public Builder binaryAsString(final @Valid Boolean binaryAsString) {
      this.binaryAsString = binaryAsString;
      return this;
    }


    /**
     * Set extractUnionsByType and return the builder.
     * If you want to operate on individual members of a union, set &#x60;extractUnionsByType&#x60; on the Avro parser. This configuration expands union values into nested objects according to the following rules: - Primitive types and unnamed complex types are keyed by their type name, such as &#x60;int&#x60; and &#x60;string&#x60;. - Named [complex types](https://avro.apache.org/docs/1.10.2/spec.html#schema_complex) are keyed by their names. This includes &#x60;record&#x60;, &#x60;fixed&#x60;, and &#x60;enum&#x60;. - The Avro null type is elided as its value can only ever be null.
     */
    @JsonProperty("extractUnionsByType")
    public Builder extractUnionsByType(final @Valid Boolean extractUnionsByType) {
      this.extractUnionsByType = extractUnionsByType;
      return this;
    }



    @Override
    public AvroStreamFormatSettings build() {
      return new AvroStreamFormatSettings(format, parseSchemaProvider, binaryAsString, extractUnionsByType);
    }
  }
}
