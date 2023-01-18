package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type",  visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = ConnectionParseSchemaProvider.class, name = "connection"),
    @JsonSubTypes.Type(value = InlineAvroParseSchemaProvider.class, name = "inline-avro"),
})
/**
 * A parse schema provider describes the source of schema information used to parse input data. These are used for input data types that have external schemas (i.e., where the schema is not stored with the data itself), such as Protobuf and Avro.
 */
@JsonDeserialize(builder = ParseSchemaProvider.Builder.class)
public class ParseSchemaProvider {


  private final @NotNull
  @Valid ParseSchemaProviderType type;

  public ParseSchemaProvider(
      final @NotNull ParseSchemaProviderType type
  ) {
    this.type = type;
  }





  @JsonProperty("type") @NotNull
  public ParseSchemaProviderType getType() {
    return type;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ParseSchemaProvider parseSchemaProvider = (ParseSchemaProvider) o;
    return Objects.equals(this.type, parseSchemaProvider.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ParseSchemaProvider {\n");

    sb.append("    type: ").append(toIndentedString(type)).append("\n");
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


  /**
   * Calls the appropriate visitor method for this type.
   * @param visitor A visitor
   */
  public void accept(final Visitor visitor) {
    throw new UnsupportedOperationException("Cannot call visitor on ParseSchemaProvider instance. Use a subclass.");
  }

  /**
   * A type-safe visitor of the sub-classes of this type.
   */
  public interface Visitor {
    void visit(ConnectionParseSchemaProvider dto);
    void visit(InlineAvroParseSchemaProvider dto);
  }

  /**
   * Interface that subtype's Builder implement, allowing setting common fields.
   */
  public interface IBuilder<T extends ParseSchemaProvider> {
    IBuilder type(ParseSchemaProviderType type);


    T build();
  }

  @JsonPOJOBuilder
  public static class Builder {
    private @Valid ParseSchemaProviderType type;

    /**
     * Set type and return the builder.
     */
    @JsonProperty("type")
    public Builder type(final @Valid  @NotNull
                        ParseSchemaProviderType type) {
      this.type = type;
      return this;
    }



    public ParseSchemaProvider build() {
      return new ParseSchemaProvider(type);
    }
  }
}
