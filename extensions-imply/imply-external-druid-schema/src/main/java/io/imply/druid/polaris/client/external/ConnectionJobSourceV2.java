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
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A source that is external to Polaris. Data in this source is pulled into Polaris.
 */
@JsonDeserialize(builder = ConnectionJobSourceV2.Builder.class)
public class ConnectionJobSourceV2 extends JobSourceV2
{


  private final @NotNull @Valid String connectionName;
  @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
  private final @Valid List<FieldNameAndDataTypeV2> inputSchema;
  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid DataFormatSettings formatSettings;

  public ConnectionJobSourceV2(
      final @NotNull JobSourceTypeV2 type,
      final String connectionName,
      final List<FieldNameAndDataTypeV2> inputSchema,
      final DataFormatSettings formatSettings
  )
  {
    super(type);
    this.connectionName = connectionName;
    this.inputSchema =
        ImmutableList.copyOf(inputSchema);
    this.formatSettings = formatSettings;
  }

  public static ConnectionJobSourceV2.Builder builder()
  {
    return new ConnectionJobSourceV2.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public ConnectionJobSourceV2.Builder with()
  {
    return (new Builder())
        .type(this.getType())
        .connectionName(this.getConnectionName())
        .inputSchema(this.getInputSchema())
        .formatSettings(this.getFormatSettings());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public ConnectionJobSourceV2 with(final java.util.function.Consumer<Builder> consumer)
  {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public ConnectionJobSourceV2 cloneWithReadOnlyDefaults()
  {
    return (new Builder())
        .connectionName(this.getConnectionName())
        .inputSchema(this.getInputSchema())
        .formatSettings(this.getFormatSettings())
        .type(this.getType())
        .build();
  }


  @JsonProperty("connectionName")
  @NotNull
  public String getConnectionName()
  {
    return connectionName;
  }

  @JsonProperty("inputSchema")
  public List<FieldNameAndDataTypeV2> getInputSchema()
  {
    return inputSchema;
  }

  @JsonProperty("formatSettings")
  public DataFormatSettings getFormatSettings()
  {
    return formatSettings;
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
    ConnectionJobSourceV2 connectionJobSourceV2 = (ConnectionJobSourceV2) o;
    return Objects.equals(this.connectionName, connectionJobSourceV2.connectionName) &&
           Objects.equals(this.inputSchema, connectionJobSourceV2.inputSchema) &&
           Objects.equals(this.formatSettings, connectionJobSourceV2.formatSettings) &&
           super.equals(o);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(connectionName, inputSchema, formatSettings, super.hashCode());
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("class ConnectionJobSourceV2 {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    connectionName: ").append(toIndentedString(connectionName)).append("\n");
    sb.append("    inputSchema: ").append(toIndentedString(inputSchema)).append("\n");
    sb.append("    formatSettings: ").append(toIndentedString(formatSettings)).append("\n");
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
  public void accept(final JobSourceV2.Visitor visitor)
  {
    visitor.visit(this);
  }


  @JsonPOJOBuilder
  public static class Builder implements JobSourceV2.IBuilder<ConnectionJobSourceV2>
  {
    private @Valid JobSourceTypeV2 type;
    private @Valid String connectionName;
    private @Valid List<FieldNameAndDataTypeV2> inputSchema = new ArrayList<>();
    private @Valid DataFormatSettings formatSettings;

    /**
     * Set type and return the builder.
     */
    @Override
    @JsonProperty("type")
    public Builder type(
        final @Valid @NotNull
        JobSourceTypeV2 type
    )
    {
      this.type = type;
      return this;
    }


    /**
     * Set connectionName and return the builder.
     * Name of the connection to ingest from.
     */
    @JsonProperty("connectionName")
    public Builder connectionName(
        final @Valid @NotNull
        String connectionName
    )
    {
      this.connectionName = connectionName;
      return this;
    }


    /**
     * Set inputSchema and return the builder.
     * The schema of input data in terms of a list of input field names and their respective data types.
     */
    @JsonProperty("inputSchema")
    public Builder inputSchema(final @Valid List<FieldNameAndDataTypeV2> inputSchema)
    {
      this.inputSchema = new ArrayList<>(inputSchema);
      return this;
    }

    public Builder addInputSchemaItem(final FieldNameAndDataTypeV2 inputSchemaItem)
    {
      if (this.inputSchema == null) {
        this.inputSchema = new ArrayList<>();
      }

      this.inputSchema.add(inputSchemaItem);
      return this;
    }

    public Builder removeInputSchemaItem(final FieldNameAndDataTypeV2 inputSchemaItem)
    {
      if (inputSchemaItem != null && this.inputSchema != null) {
        this.inputSchema.remove(inputSchemaItem);
      }

      return this;
    }

    /**
     * Set formatSettings and return the builder.
     */
    @JsonProperty("formatSettings")
    public Builder formatSettings(final @Valid DataFormatSettings formatSettings)
    {
      this.formatSettings = formatSettings;
      return this;
    }


    @Override
    public ConnectionJobSourceV2 build()
    {
      return new ConnectionJobSourceV2(type, connectionName, inputSchema, formatSettings);
    }
  }
}
