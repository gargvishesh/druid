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

@JsonDeserialize(builder = S3JobSourceV2.Builder.class)
public class S3JobSourceV2 extends JobSourceV2 {


  private final @NotNull
  @Valid String connectionName;
  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid List<String> uris;
  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid List<String> prefixes;
  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid List<String> objects;
  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid List<FieldNameAndDataTypeV2> inputSchema;

  private final @NotNull @Valid DataFormatSettings formatSettings;

  public S3JobSourceV2(
      final @NotNull JobSourceTypeV2 type,
      final String connectionName,
      final List<String> uris,
      final List<String> prefixes,
      final List<String> objects,
      final List<FieldNameAndDataTypeV2> inputSchema,
      final @NotNull DataFormatSettings formatSettings
  ) {
    super(type);
    this.connectionName = connectionName;
    this.uris =
        ImmutableList.copyOf(uris);
    this.prefixes =
        ImmutableList.copyOf(prefixes);
    this.objects =
        ImmutableList.copyOf(objects);
    this.inputSchema =
        ImmutableList.copyOf(inputSchema);
    this.formatSettings = formatSettings;
  }

  public static S3JobSourceV2.Builder builder() {
    return new S3JobSourceV2.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public S3JobSourceV2.Builder with() {
    return (new Builder())
        .type(this.getType())
        .connectionName(this.getConnectionName())
        .uris(this.getUris())
        .prefixes(this.getPrefixes())
        .objects(this.getObjects())
        .inputSchema(this.getInputSchema())
        .formatSettings(this.getFormatSettings());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public S3JobSourceV2 with(final java.util.function.Consumer<Builder> consumer) {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public S3JobSourceV2 cloneWithReadOnlyDefaults() {
    return (new Builder())
        .connectionName(this.getConnectionName())
        .uris(this.getUris())
        .prefixes(this.getPrefixes())
        .objects(this.getObjects())
        .inputSchema(this.getInputSchema())
        .formatSettings(this.getFormatSettings())
        .type(this.getType())
        .build();
  }




  @JsonProperty("connectionName") @NotNull
  public String getConnectionName() {
    return connectionName;
  }

  @JsonProperty("uris")
  public List<String> getUris() {
    return uris;
  }

  @JsonProperty("prefixes")
  public List<String> getPrefixes() {
    return prefixes;
  }

  @JsonProperty("objects")
  public List<String> getObjects() {
    return objects;
  }

  @JsonProperty("inputSchema")
  public List<FieldNameAndDataTypeV2> getInputSchema() {
    return inputSchema;
  }

  @JsonProperty("formatSettings") @NotNull
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
    S3JobSourceV2 s3JobSourceV2 = (S3JobSourceV2) o;
    return Objects.equals(this.connectionName, s3JobSourceV2.connectionName) &&
           Objects.equals(this.uris, s3JobSourceV2.uris) &&
           Objects.equals(this.prefixes, s3JobSourceV2.prefixes) &&
           Objects.equals(this.objects, s3JobSourceV2.objects) &&
           Objects.equals(this.inputSchema, s3JobSourceV2.inputSchema) &&
           Objects.equals(this.formatSettings, s3JobSourceV2.formatSettings) &&
           super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionName, uris, prefixes, objects, inputSchema, formatSettings, super.hashCode());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class S3JobSourceV2 {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    connectionName: ").append(toIndentedString(connectionName)).append("\n");
    sb.append("    uris: ").append(toIndentedString(uris)).append("\n");
    sb.append("    prefixes: ").append(toIndentedString(prefixes)).append("\n");
    sb.append("    objects: ").append(toIndentedString(objects)).append("\n");
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
  public static class Builder implements JobSourceV2.IBuilder<S3JobSourceV2> {
    private @Valid JobSourceTypeV2 type;
    private @Valid String connectionName;
    private @Valid List<String> uris = new ArrayList<>();
    private @Valid List<String> prefixes = new ArrayList<>();
    private @Valid List<String> objects = new ArrayList<>();
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
     * Set connectionName and return the builder.
     * Name of the connection to ingest from.
     */
    @JsonProperty("connectionName")
    public Builder connectionName(final @Valid  @NotNull
                                  String connectionName) {
      this.connectionName = connectionName;
      return this;
    }


    /**
     * Set uris and return the builder.
     * JSON array containing URIs of S3 objects to be ingested.
     */
    @JsonProperty("uris")
    public Builder uris(final @Valid List<String> uris) {
      this.uris = new ArrayList<>(uris);
      return this;
    }

    public Builder addUrisItem(final String urisItem) {
      if (this.uris == null) {
        this.uris = new ArrayList<>();
      }

      this.uris.add(urisItem);
      return this;
    }

    public Builder removeUrisItem(final String urisItem) {
      if (urisItem != null && this.uris != null) {
        this.uris.remove(urisItem);
      }

      return this;
    }

    /**
     * Set prefixes and return the builder.
     * JSON array containing URI prefixes for S3 buckets and folders. Polaris will ingest everything in the specified bucket or folder aside from empty objects.
     */
    @JsonProperty("prefixes")
    public Builder prefixes(final @Valid List<String> prefixes) {
      this.prefixes = new ArrayList<>(prefixes);
      return this;
    }

    public Builder addPrefixesItem(final String prefixesItem) {
      if (this.prefixes == null) {
        this.prefixes = new ArrayList<>();
      }

      this.prefixes.add(prefixesItem);
      return this;
    }

    public Builder removePrefixesItem(final String prefixesItem) {
      if (prefixesItem != null && this.prefixes != null) {
        this.prefixes.remove(prefixesItem);
      }

      return this;
    }

    /**
     * Set objects and return the builder.
     * JSON array of S3 objects to be ingested from the connection.
     */
    @JsonProperty("objects")
    public Builder objects(final @Valid List<String> objects) {
      this.objects = new ArrayList<>(objects);
      return this;
    }

    public Builder addObjectsItem(final String objectsItem) {
      if (this.objects == null) {
        this.objects = new ArrayList<>();
      }

      this.objects.add(objectsItem);
      return this;
    }

    public Builder removeObjectsItem(final String objectsItem) {
      if (objectsItem != null && this.objects != null) {
        this.objects.remove(objectsItem);
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
    public Builder formatSettings(final @Valid  @NotNull
                                  DataFormatSettings formatSettings) {
      this.formatSettings = formatSettings;
      return this;
    }



    @Override
    public S3JobSourceV2 build() {
      return new S3JobSourceV2(type, connectionName, uris, prefixes, objects, inputSchema, formatSettings);
    }
  }
}
