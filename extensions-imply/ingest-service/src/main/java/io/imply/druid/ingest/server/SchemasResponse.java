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
import io.imply.druid.ingest.metadata.StoredIngestSchema;

import java.util.List;
import java.util.Objects;

public class SchemasResponse
{
  private final List<StoredIngestSchema> schemas;

  @JsonCreator
  public SchemasResponse(
      @JsonProperty("schemas") List<StoredIngestSchema> schemas
  )
  {
    this.schemas = schemas;
  }

  @JsonProperty
  public List<StoredIngestSchema> getSchemas()
  {
    return schemas;
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
    SchemasResponse that = (SchemasResponse) o;
    return Objects.equals(schemas, that.schemas);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(schemas);
  }

  @Override
  public String toString()
  {
    return "SchemasResponse{" +
           "schemas=" + schemas +
           '}';
  }
}
