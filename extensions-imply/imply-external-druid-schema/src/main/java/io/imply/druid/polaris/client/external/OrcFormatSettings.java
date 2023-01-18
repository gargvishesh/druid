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
import org.apache.druid.java.util.common.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@JsonDeserialize(builder = OrcFormatSettings.Builder.class)
public class OrcFormatSettings extends DataFormatSettings
{

  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid Boolean binaryAsString;

  public OrcFormatSettings(
      final @NotNull DataFormat format,
      final Boolean binaryAsString
  )
  {
    super(format);
    this.binaryAsString = binaryAsString;
  }

  public static OrcFormatSettings.Builder builder()
  {
    return new OrcFormatSettings.Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public OrcFormatSettings.Builder with()
  {
    return (new Builder())
        .format(this.getFormat())
        .binaryAsString(this.getBinaryAsString());
  }

  /**
   * Return a new instance using the provided consumer to configure the builder.
   *
   * @param consumer A method that receives the builder and updates it
   */
  public OrcFormatSettings with(final java.util.function.Consumer<Builder> consumer)
  {
    final Builder builder = with();
    consumer.accept(builder);
    return builder.build();
  }

  /**
   * Return a clone of this instance with readOnly fields reset to their defaults.
   */
  public OrcFormatSettings cloneWithReadOnlyDefaults()
  {
    return (new Builder())
        .binaryAsString(this.getBinaryAsString())
        .format(this.getFormat())
        .build();
  }


  @JsonProperty("binaryAsString")
  public Boolean getBinaryAsString()
  {
    return binaryAsString;
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
    OrcFormatSettings orcFormatSettings = (OrcFormatSettings) o;
    return Objects.equals(this.binaryAsString, orcFormatSettings.binaryAsString) &&
           super.equals(o);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(binaryAsString, super.hashCode());
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("class OrcFormatSettings {\n");
    sb.append("    ").append(toIndentedString(super.toString())).append("\n");
    sb.append("    binaryAsString: ").append(toIndentedString(binaryAsString)).append("\n");
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
  public void accept(final DataFormatSettings.Visitor visitor)
  {
    visitor.visit(this);
  }


  @JsonPOJOBuilder
  public static class Builder implements DataFormatSettings.IBuilder<OrcFormatSettings>
  {
    private @Valid DataFormat format;
    private @Valid Boolean binaryAsString = false;

    /**
     * Set format and return the builder.
     */
    @Override
    @JsonProperty("format")
    public Builder format(
        final @Valid @NotNull
        DataFormat format
    )
    {
      this.format = format;
      return this;
    }


    /**
     * Set binaryAsString and return the builder.
     * Specifies if the binary orc column which is not logically marked as a string should be treated as a UTF-8 encoded string.
     */
    @JsonProperty("binaryAsString")
    public Builder binaryAsString(final @Valid Boolean binaryAsString)
    {
      this.binaryAsString = binaryAsString;
      return this;
    }


    @Override
    public OrcFormatSettings build()
    {
      return new OrcFormatSettings(format, binaryAsString);
    }
  }
}
