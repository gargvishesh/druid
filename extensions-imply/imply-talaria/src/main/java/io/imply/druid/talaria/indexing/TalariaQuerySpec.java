/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.query.Query;

import javax.annotation.Nullable;
import java.util.Objects;

public class TalariaQuerySpec
{
  private final Query<?> query;
  private final ColumnMappings columnMappings;
  private final TalariaDestination destination;
  private final ParallelIndexTuningConfig tuningConfig;

  @JsonCreator
  public TalariaQuerySpec(
      @JsonProperty("query") Query<?> query,
      @JsonProperty("columnMappings") @Nullable ColumnMappings columnMappings,
      @JsonProperty("destination") TalariaDestination destination,
      @JsonProperty("tuningConfig") ParallelIndexTuningConfig tuningConfig
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.columnMappings = Preconditions.checkNotNull(columnMappings, "columnMappings");
    this.destination = Preconditions.checkNotNull(destination, "destination");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
  }

  @JsonProperty
  public Query<?> getQuery()
  {
    return query;
  }

  @JsonProperty("columnMappings")
  public ColumnMappings getColumnMappings()
  {
    return columnMappings;
  }

  @JsonProperty
  public TalariaDestination getDestination()
  {
    return destination;
  }

  @JsonProperty
  public ParallelIndexTuningConfig getTuningConfig()
  {
    return tuningConfig;
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
    TalariaQuerySpec that = (TalariaQuerySpec) o;
    return Objects.equals(query, that.query)
           && Objects.equals(columnMappings, that.columnMappings)
           && Objects.equals(destination, that.destination)
           && Objects.equals(tuningConfig, that.tuningConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        query,
        columnMappings,
        destination,
        tuningConfig
    );
  }

  @Override
  public String toString()
  {
    return "TalariaQuerySpec{" +
           "query=" + query +
           ", columnMappings=" + columnMappings +
           ", destination=" + destination +
           ", tuningConfig=" + tuningConfig +
           '}';
  }
}
