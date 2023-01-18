package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@JsonDeserialize(builder = JsonFormatSettings.Builder.class)
public class JsonFormatSettings extends DataFormatSettings {


  public JsonFormatSettings(
      final @NotNull DataFormat format
  ) {
    super(format);
  }

  public static JsonFormatSettings.Builder builder() {
    return new JsonFormatSettings.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public JsonFormatSettings.Builder with() {
    return (new Builder())
        .format(this.getFormat());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public JsonFormatSettings with(final java.util.function.Consumer<Builder> consumer) {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public JsonFormatSettings cloneWithReadOnlyDefaults() {
    return (new Builder())
        .format(this.getFormat())
        .build();
  }




  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class JsonFormatSettings {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
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
  public static class Builder implements DataFormatSettings.IBuilder<JsonFormatSettings> {
    private @Valid DataFormat format;

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



    @Override
    public JsonFormatSettings build() {
      return new JsonFormatSettings(format);
    }
  }
}
