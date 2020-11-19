/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

public class IngestServiceSqlMetatadataConfig
{
  public static final IngestServiceSqlMetatadataConfig DEFAULT_CONFIG =
      new IngestServiceSqlMetatadataConfig(null, null, null);

  @JsonProperty("createTables")
  private final boolean createTables;

  @JsonProperty("jobs")
  private final String jobsTable;

  @JsonProperty("tables")
  private final String tablesTable;

  @JsonCreator
  public IngestServiceSqlMetatadataConfig(
      @JsonProperty("createTables") @Nullable Boolean createTables,
      @JsonProperty("jobs") String jobsTable,
      @JsonProperty("tables") String tablesTable
  )
  {
    this.createTables = createTables == null ? true : createTables;
    this.tablesTable = tablesTable == null ? "ingest_tables" : tablesTable;
    this.jobsTable = jobsTable == null ? "ingest_jobs" : jobsTable;
  }

  public String getJobsTable()
  {
    return jobsTable;
  }

  public String getTablesTable()
  {
    return tablesTable;
  }

  public boolean shouldCreateTables()
  {
    return createTables;
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
    IngestServiceSqlMetatadataConfig that = (IngestServiceSqlMetatadataConfig) o;
    return createTables == that.createTables &&
           Objects.equals(jobsTable, that.jobsTable) &&
           Objects.equals(tablesTable, that.tablesTable);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(createTables, jobsTable, tablesTable);
  }
}
