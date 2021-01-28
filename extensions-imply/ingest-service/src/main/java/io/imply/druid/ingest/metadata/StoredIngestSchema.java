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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;

import javax.annotation.Nullable;
import java.util.Objects;

public class StoredIngestSchema extends IngestSchema
{
  private final Integer schemaId;

  public StoredIngestSchema(Integer schemaId, IngestSchema blob)
  {
    this(
        schemaId,
        blob.getTimestampSpec(),
        blob.getDimensionsSpec(),
        blob.getPartitionScheme(),
        blob.getInputFormat(),
        blob.getDescription()
    );
  }

  @JsonCreator
  public StoredIngestSchema(
      @JsonProperty("schemaId") Integer schemaId,
      @JsonProperty("timestampSpec") @Nullable TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") @Nullable DimensionsSpec dimensionsSpec,
      @JsonProperty("partitionScheme")@Nullable PartitionScheme partitionScheme,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("description") @Nullable String description
  )
  {
    super(timestampSpec, dimensionsSpec, partitionScheme, inputFormat, description);
    this.schemaId = schemaId;
  }

  @JsonProperty("schemaId")
  public Integer getSchemaId()
  {
    return schemaId;
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
    if (!super.equals(o)) {
      return false;
    }
    StoredIngestSchema that = (StoredIngestSchema) o;
    return Objects.equals(schemaId, that.schemaId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), schemaId);
  }

  @Override
  public String toString()
  {
    return "StoredIngestSchema{" +
           "timestampSpec=" + timestampSpec +
           ", dimensionsSpec=" + dimensionsSpec +
           ", partitionScheme=" + partitionScheme +
           ", inputFormat=" + inputFormat +
           ", description='" + description + '\'' +
           ", schemaId=" + schemaId +
           '}';
  }
}
