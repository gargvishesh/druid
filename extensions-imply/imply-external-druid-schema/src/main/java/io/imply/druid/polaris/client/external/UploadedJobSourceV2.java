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

/**
 * A source input comprised of a set of files uploaded to Imply Polaris.
 */
@JsonDeserialize(builder = UploadedJobSourceV2.Builder.class)
public class UploadedJobSourceV2 extends JobSourceV2 {

  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid List<String> fileList;
  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid List<FieldNameAndDataTypeV2> inputSchema;
  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid DataFormatSettings formatSettings;

  public UploadedJobSourceV2(
      final @NotNull JobSourceTypeV2 type,
      final List<String> fileList,
      final List<FieldNameAndDataTypeV2> inputSchema,
      final DataFormatSettings formatSettings
  ) {
    super(type);
    this.fileList =
        ImmutableList.copyOf(fileList);
    this.inputSchema =
        ImmutableList.copyOf(inputSchema);
    this.formatSettings = formatSettings;
  }

  public static UploadedJobSourceV2.Builder builder() {
    return new UploadedJobSourceV2.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public UploadedJobSourceV2.Builder with() {
    return (new Builder())
        .type(this.getType())
        .fileList(this.getFileList())
        .inputSchema(this.getInputSchema())
        .formatSettings(this.getFormatSettings());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public UploadedJobSourceV2 with(final java.util.function.Consumer<Builder> consumer) {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public UploadedJobSourceV2 cloneWithReadOnlyDefaults() {
    return (new Builder())
        .fileList(this.getFileList())
        .inputSchema(this.getInputSchema())
        .formatSettings(this.getFormatSettings())
        .type(this.getType())
        .build();
  }




  @JsonProperty("fileList")
  public List<String> getFileList() {
    return fileList;
  }

  @JsonProperty("inputSchema")
  public List<FieldNameAndDataTypeV2> getInputSchema() {
    return inputSchema;
  }

  @JsonProperty("formatSettings")
  public DataFormatSettings getFormatSettings() {
    return formatSettings;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UploadedJobSourceV2 uploadedJobSourceV2 = (UploadedJobSourceV2) o;
    return Objects.equals(this.fileList, uploadedJobSourceV2.fileList) &&
           Objects.equals(this.inputSchema, uploadedJobSourceV2.inputSchema) &&
           Objects.equals(this.formatSettings, uploadedJobSourceV2.formatSettings) &&
           super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileList, inputSchema, formatSettings, super.hashCode());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UploadedJobSourceV2 {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    fileList: ").append(toIndentedString(fileList)).append("\n");
    sb.append("    inputSchema: ").append(toIndentedString(inputSchema)).append("\n");
    sb.append("    formatSettings: ").append(toIndentedString(formatSettings)).append("\n");
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
  public void accept(final JobSourceV2.Visitor visitor) {
    visitor.visit(this);
  }


  @JsonPOJOBuilder
  public static class Builder implements JobSourceV2.IBuilder<UploadedJobSourceV2> {
    private @Valid JobSourceTypeV2 type;
    private @Valid List<String> fileList = new ArrayList<>();
    private @Valid List<FieldNameAndDataTypeV2> inputSchema = new ArrayList<>();
    private @Valid DataFormatSettings formatSettings;

    /**
     * Set type and return the builder.
     */
    @Override
    @JsonProperty("type")
    public Builder type(final @Valid  @NotNull
                        JobSourceTypeV2 type) {
      this.type = type;
      return this;
    }


    /**
     * Set fileList and return the builder.
     * List of files to ingest. All files must have the same format, for example, newline-delimited JSON. To ingest files with different formats or format settings, split into multiple ingestion jobs.
     */
    @JsonProperty("fileList")
    public Builder fileList(final @Valid List<String> fileList) {
      this.fileList = new ArrayList<>(fileList);
      return this;
    }

    public Builder addFileListItem(final String fileListItem) {
      if (this.fileList == null) {
        this.fileList = new ArrayList<>();
      }

      this.fileList.add(fileListItem);
      return this;
    }

    public Builder removeFileListItem(final String fileListItem) {
      if (fileListItem != null && this.fileList != null) {
        this.fileList.remove(fileListItem);
      }

      return this;
    }

    /**
     * Set inputSchema and return the builder.
     * The schema of input data in terms of a list of input field names and their respective data types.
     */
    @JsonProperty("inputSchema")
    public Builder inputSchema(final @Valid List<FieldNameAndDataTypeV2> inputSchema) {
      this.inputSchema = new ArrayList<>(inputSchema);
      return this;
    }

    public Builder addInputSchemaItem(final FieldNameAndDataTypeV2 inputSchemaItem) {
      if (this.inputSchema == null) {
        this.inputSchema = new ArrayList<>();
      }

      this.inputSchema.add(inputSchemaItem);
      return this;
    }

    public Builder removeInputSchemaItem(final FieldNameAndDataTypeV2 inputSchemaItem) {
      if (inputSchemaItem != null && this.inputSchema != null) {
        this.inputSchema.remove(inputSchemaItem);
      }

      return this;
    }

    /**
     * Set formatSettings and return the builder.
     */
    @JsonProperty("formatSettings")
    public Builder formatSettings(final @Valid DataFormatSettings formatSettings) {
      this.formatSettings = formatSettings;
      return this;
    }



    @Override
    public UploadedJobSourceV2 build() {
      return new UploadedJobSourceV2(type, fileList, inputSchema, formatSettings);
    }
  }
}
