/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.druid.java.util.common.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = ConnectionJobSourceV2.class, name = "connection"),
    @JsonSubTypes.Type(value = S3JobSourceV2.class, name = "s3"),
    @JsonSubTypes.Type(value = UploadedJobSourceV2.class, name = "uploaded"),
})
/**
 * The source input of the job.
 */
@JsonDeserialize(builder = JobSourceV2.Builder.class)
public class JobSourceV2
{
  private final @NotNull @Valid JobSourceTypeV2 type;

  public JobSourceV2(
      final @NotNull JobSourceTypeV2 type
  )
  {
    this.type = type;
  }


  @JsonProperty("type")
  @NotNull
  public JobSourceTypeV2 getType()
  {
    return type;
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
    JobSourceV2 jobSourceV2 = (JobSourceV2) o;
    return Objects.equals(this.type, jobSourceV2.type);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type);
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("class JobSourceV2 {\n");

    sb.append("    type: ").append(toIndentedString(type)).append("\n");
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


  /**
   * Calls the appropriate visitor method for this type.
   *
   * @param visitor A visitor
   */
  public void accept(final Visitor visitor)
  {
    throw new UnsupportedOperationException("Cannot call visitor on JobSourceV2 instance. Use a subclass.");
  }

  /**
   * A type-safe visitor of the sub-classes of this type.
   */
  public interface Visitor
  {
    void visit(ConnectionJobSourceV2 dto);

    void visit(S3JobSourceV2 dto);

    void visit(UploadedJobSourceV2 dto);
  }

  /**
   * Interface that subtype's Builder implement, allowing setting common fields.
   */
  public interface IBuilder<T extends JobSourceV2>
  {
    IBuilder type(JobSourceTypeV2 type);


    T build();
  }

  @JsonPOJOBuilder
  public static class Builder
  {
    private @Valid JobSourceTypeV2 type;

    /**
     * Set type and return the builder.
     */
    @JsonProperty("type")
    public Builder type(
        final @Valid @NotNull
        JobSourceTypeV2 type
    )
    {
      this.type = type;
      return this;
    }


    public JobSourceV2 build()
    {
      return new JobSourceV2(type);
    }
  }
}
