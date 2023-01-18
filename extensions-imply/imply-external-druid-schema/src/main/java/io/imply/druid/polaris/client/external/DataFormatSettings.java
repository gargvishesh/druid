package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "format",  visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = AvroOcfFormatSettings.class, name = "avro_ocf"),
    @JsonSubTypes.Type(value = AvroStreamFormatSettings.class, name = "avro_stream"),
    @JsonSubTypes.Type(value = CsvFormatSettings.class, name = "csv"),
    @JsonSubTypes.Type(value = JsonFormatSettings.class, name = "nd-json"),
    @JsonSubTypes.Type(value = OrcFormatSettings.class, name = "orc"),
    @JsonSubTypes.Type(value = ParquetFormatSettings.class, name = "parquet"),
})
/**
 * Data format settings that apply to all files in the ingestion job. Polaris automatically detects the file type based on the file extension. If you specify a value that does not match the automatically detected type, Polaris attempts to ingest based on the user-specified value.
 */
@JsonDeserialize(builder = DataFormatSettings.Builder.class)
public class DataFormatSettings {


  private final @NotNull
  @Valid DataFormat format;

  public DataFormatSettings(
      final @NotNull DataFormat format
  ) {
    this.format = format;
  }





  @JsonProperty("format") @NotNull
  public DataFormat getFormat() {
    return format;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataFormatSettings dataFormatSettings = (DataFormatSettings) o;
    return Objects.equals(this.format, dataFormatSettings.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DataFormatSettings {\n");

    sb.append("    format: ").append(toIndentedString(format)).append("\n");
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
    throw new UnsupportedOperationException("Cannot call visitor on DataFormatSettings instance. Use a subclass.");
  }

  /**
   * A type-safe visitor of the sub-classes of this type.
   */
  public interface Visitor {
    void visit(AvroOcfFormatSettings dto);
    void visit(AvroStreamFormatSettings dto);
    void visit(CsvFormatSettings dto);
    void visit(JsonFormatSettings dto);
    void visit(OrcFormatSettings dto);
    void visit(ParquetFormatSettings dto);
  }

  /**
   * Interface that subtype's Builder implement, allowing setting common fields.
   */
  public interface IBuilder<T extends DataFormatSettings> {
    IBuilder format(DataFormat format);


    T build();
  }

  @JsonPOJOBuilder
  public static class Builder {
    private @Valid DataFormat format;

    /**
     * Set format and return the builder.
     */
    @JsonProperty("format")
    public Builder format(final @Valid  @NotNull
                          DataFormat format) {
      this.format = format;
      return this;
    }



    public DataFormatSettings build() {
      return new DataFormatSettings(format);
    }
  }
}
