/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Ingestion schema which defines how an {@link IngestJob} might load data into a table. Decorated for JSON
 * serialization so that it can be persisted as a JSON blob, as well as be used directly by API requests and responses.
 */
public class IngestSchema
{
  protected final TimestampSpec timestampSpec;
  protected final DimensionsSpec dimensionsSpec;
  protected final PartitionScheme partitionScheme;
  protected final InputFormat inputFormat;
  protected final String description;

  @JsonCreator
  public IngestSchema(
      @JsonProperty("timestampSpec") @Nullable TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") @Nullable DimensionsSpec dimensionsSpec,
      @JsonProperty("partitionScheme") @Nullable PartitionScheme partitionScheme,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("description") @Nullable String description
  )
  {
    this.timestampSpec = timestampSpec == null
                         ? new TimestampSpec(null, null, null)
                         : timestampSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.partitionScheme = partitionScheme == null ? new PartitionScheme(null, null) : partitionScheme;
    this.inputFormat = Preconditions.checkNotNull(inputFormat, "'inputFormat' must be specified");
    this.description = description;
  }

  @JsonProperty("timestampSpec")
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @Nullable
  @JsonProperty("dimensionsSpec")
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty("partitionScheme")
  public PartitionScheme getPartitionScheme()
  {
    return partitionScheme;
  }

  @JsonProperty("inputFormat")
  public InputFormat getInputFormat()
  {
    return inputFormat;
  }

  @Nullable
  @JsonProperty("description")
  public String getDescription()
  {
    return description;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IngestSchema that = (IngestSchema) o;
    return Objects.equals(timestampSpec, that.timestampSpec) &&
           Objects.equals(dimensionsSpec, that.dimensionsSpec) &&
           Objects.equals(partitionScheme, that.partitionScheme) &&
           Objects.equals(inputFormat, that.inputFormat) &&
           Objects.equals(description, that.description);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timestampSpec, dimensionsSpec, partitionScheme, inputFormat, description);
  }

  @Override
  public String toString()
  {
    return "IngestSchema{" +
           "timestampSpec=" + timestampSpec +
           ", dimensionsSpec=" + dimensionsSpec +
           ", partitionScheme=" + partitionScheme +
           ", inputFormat=" + inputFormat +
           ", description='" + description + '\'' +
           '}';
  }
}
