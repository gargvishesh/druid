package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableList;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonDeserialize(builder = CsvFormatSettings.Builder.class)
public class CsvFormatSettings extends DataFormatSettings {

  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid String delimiter;
  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid Integer skipHeaderRows;
  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid List<String> columns;

  public CsvFormatSettings(
      final @NotNull DataFormat format,
      final String delimiter,
      final Integer skipHeaderRows,
      final List<String> columns
  ) {
    super(format);
    this.delimiter = delimiter;
    this.skipHeaderRows = skipHeaderRows;
    this.columns =
        ImmutableList.copyOf(columns);
  }

  public static CsvFormatSettings.Builder builder() {
    return new CsvFormatSettings.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public CsvFormatSettings.Builder with() {
    return (new Builder())
        .format(this.getFormat())
        .delimiter(this.getDelimiter())
        .skipHeaderRows(this.getSkipHeaderRows())
        .columns(this.getColumns());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public CsvFormatSettings with(final java.util.function.Consumer<Builder> consumer) {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public CsvFormatSettings cloneWithReadOnlyDefaults() {
    return (new Builder())
        .delimiter(this.getDelimiter())
        .skipHeaderRows(this.getSkipHeaderRows())
        .columns(this.getColumns())
        .format(this.getFormat())
        .build();
  }




  @JsonProperty("delimiter")
  public String getDelimiter() {
    return delimiter;
  }

  @JsonProperty("skipHeaderRows")
  public Integer getSkipHeaderRows() {
    return skipHeaderRows;
  }

  @JsonProperty("columns")
  public List<String> getColumns() {
    return columns;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CsvFormatSettings csvFormatSettings = (CsvFormatSettings) o;
    return Objects.equals(this.delimiter, csvFormatSettings.delimiter) &&
           Objects.equals(this.skipHeaderRows, csvFormatSettings.skipHeaderRows) &&
           Objects.equals(this.columns, csvFormatSettings.columns) &&
           super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delimiter, skipHeaderRows, columns, super.hashCode());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CsvFormatSettings {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    delimiter: ").append(toIndentedString(delimiter)).append("\n");
    sb.append("    skipHeaderRows: ").append(toIndentedString(skipHeaderRows)).append("\n");
    sb.append("    columns: ").append(toIndentedString(columns)).append("\n");
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
  public static class Builder implements DataFormatSettings.IBuilder<CsvFormatSettings> {
    private @Valid DataFormat format;
    private @Valid String delimiter;
    private @Valid Integer skipHeaderRows = 0;
    private @Valid List<String> columns = new ArrayList<>();

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
     * Set delimiter and return the builder.
     * The delimiter between values. Defaults to &#x60;,&#x60; for comma-separated values. Set to &#x60;\\t&#x60; or a literal tab character for tab-separated values.
     */
    @JsonProperty("delimiter")
    public Builder delimiter(final @Valid String delimiter) {
      this.delimiter = delimiter;
      return this;
    }


    /**
     * Set skipHeaderRows and return the builder.
     * The number of initial rows to skip. Use &#x60;skipHeaderRows&#x60; if your data has multiple header rows and you only want to read the last row, or you want to supply your own column names. For streaming ingestion jobs, Polaris does not support skipping header rows and raises an error if &#x60;skipHeaderRows&#x60; is set.  Interacts with the &#x60;columns&#x60; setting. If &#x60;columns&#x60; is not set or is empty, then Polaris reads the file as follows: * Skip &#x60;skipHeaderRows&#x60; rows * Read header from the next row, row number &#x60;skipHeaderRows + 1&#x60; * Read data starting on row &#x60;skipHeaderRows + 2&#x60;  If &#x60;columns&#x60; is set, then Polaris reads the file as follows: * Skip &#x60;skipHeaderRows&#x60; rows * Read data starting on row &#x60;skipHeaderRows + 1&#x60;  With the default setting of &#x60;skipHeaderRows&#x60; set to zero, the following applies: * If &#x60;columns&#x60; is unset, then the first row is read as the header, and the data begins on the second row. * If &#x60;columns&#x60; is set, then no header row is read, and the data begins on the first row of the file.
     */
    @JsonProperty("skipHeaderRows")
    public Builder skipHeaderRows(final @Valid Integer skipHeaderRows) {
      this.skipHeaderRows = skipHeaderRows;
      return this;
    }


    /**
     * Set columns and return the builder.
     * If provided, a list of the names to use for the columns. Use this setting with files that don&#39;t contain a header row with column names. Another use case is when you want to ignore the header rows in the files using &#x60;skipHeaderRows&#x60; and supply your own column names. For streaming ingestion jobs, &#x60;columns&#x60; is required and must be non-empty.
     */
    @JsonProperty("columns")
    public Builder columns(final @Valid List<String> columns) {
      this.columns = new ArrayList<>(columns);
      return this;
    }

    public Builder addColumnsItem(final String columnsItem) {
      if (this.columns == null) {
        this.columns = new ArrayList<>();
      }

      this.columns.add(columnsItem);
      return this;
    }

    public Builder removeColumnsItem(final String columnsItem) {
      if (columnsItem != null && this.columns != null) {
        this.columns.remove(columnsItem);
      }

      return this;
    }


    @Override
    public CsvFormatSettings build() {
      return new CsvFormatSettings(format, delimiter, skipHeaderRows, columns);
    }
  }
}
