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
import io.imply.druid.talaria.kernel.WorkerAssignmentStrategy;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.query.Query;

import javax.annotation.Nullable;
import java.util.Objects;

public class TalariaQuerySpec
{
  private final Query<?> query;
  private final ColumnMappings columnMappings;
  private final MSQDestination destination;
  private final WorkerAssignmentStrategy assignmentStrategy;
  private final ParallelIndexTuningConfig tuningConfig;

  @JsonCreator
  public TalariaQuerySpec(
      @JsonProperty("query") Query<?> query,
      @JsonProperty("columnMappings") @Nullable ColumnMappings columnMappings,
      @JsonProperty("destination") MSQDestination destination,
      @JsonProperty("assignmentStrategy") WorkerAssignmentStrategy assignmentStrategy,
      @JsonProperty("tuningConfig") ParallelIndexTuningConfig tuningConfig
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.columnMappings = Preconditions.checkNotNull(columnMappings, "columnMappings");
    this.destination = Preconditions.checkNotNull(destination, "destination");
    this.assignmentStrategy = Preconditions.checkNotNull(assignmentStrategy, "assignmentStrategy");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
  }

  public static Builder builder()
  {
    return new Builder();
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
  public MSQDestination getDestination()
  {
    return destination;
  }

  @JsonProperty
  public WorkerAssignmentStrategy getAssignmentStrategy()
  {
    return assignmentStrategy;
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
           && Objects.equals(assignmentStrategy, that.assignmentStrategy)
           && Objects.equals(tuningConfig, that.tuningConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(query, columnMappings, destination, assignmentStrategy, tuningConfig);
  }

  public static class Builder
  {
    private Query<?> query;
    private ColumnMappings columnMappings;
    private MSQDestination destination = TaskReportMSQDestination.instance();
    private WorkerAssignmentStrategy assignmentStrategy = WorkerAssignmentStrategy.MAX;
    private ParallelIndexTuningConfig tuningConfig;

    public Builder query(Query<?> query)
    {
      this.query = query;
      return this;
    }

    public Builder columnMappings(final ColumnMappings columnMappings)
    {
      this.columnMappings = columnMappings;
      return this;
    }

    public Builder destination(final MSQDestination destination)
    {
      this.destination = destination;
      return this;
    }

    public Builder assignmentStrategy(final WorkerAssignmentStrategy assignmentStrategy)
    {
      this.assignmentStrategy = assignmentStrategy;
      return this;
    }

    public Builder tuningConfig(final ParallelIndexTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    public TalariaQuerySpec build()
    {
      if (destination == null) {
        destination = TaskReportMSQDestination.instance();
      }

      return new TalariaQuerySpec(query, columnMappings, destination, assignmentStrategy, tuningConfig);
    }
  }
}
