/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.ingest.metadata.IngestSchema;

import javax.annotation.Nullable;
import java.util.Objects;

public class IngestJobRequest
{
  private final IngestSchema schema;
  private final Integer schemaId;

  @JsonCreator
  public IngestJobRequest(
      @JsonProperty("schema") @Nullable IngestSchema schema,
      @JsonProperty("schemaId") @Nullable Integer schemaId
  )
  {
    this.schema = schema;
    this.schemaId = schemaId;
  }

  @Nullable
  @JsonProperty("schema")
  public IngestSchema getSchema()
  {
    return schema;
  }

  @Nullable
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
    IngestJobRequest jobSpec = (IngestJobRequest) o;
    return Objects.equals(schema, jobSpec.schema) &&
           Objects.equals(schemaId, jobSpec.schemaId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(schema, schemaId);
  }
}
