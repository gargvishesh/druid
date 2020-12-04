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
      new IngestServiceSqlMetatadataConfig(null, null, null, null);

  @JsonProperty("createTables")
  private final boolean createTables;

  @JsonProperty("jobs")
  private final String jobsTable;

  @JsonProperty("tables")
  private final String tablesTable;

  @JsonProperty("schemas")
  private final String schemasTable;

  @JsonCreator
  public IngestServiceSqlMetatadataConfig(
      @JsonProperty("createTables") @Nullable Boolean createTables,
      @JsonProperty("jobs") String jobsTable,
      @JsonProperty("tables") String tablesTable,
      @JsonProperty("schemas") String schemasTable
  )
  {
    this.createTables = createTables == null ? true : createTables;
    this.tablesTable = tablesTable == null ? "ingest_tables" : tablesTable;
    this.jobsTable = jobsTable == null ? "ingest_jobs" : jobsTable;
    this.schemasTable = schemasTable == null ? "ingest_schemas" : schemasTable;
  }

  public String getJobsTable()
  {
    return jobsTable;
  }

  public String getTablesTable()
  {
    return tablesTable;
  }

  public String getSchemasTable()
  {
    return schemasTable;
  }

  public boolean isCreateTables()
  {
    return createTables;
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
    return isCreateTables() == that.isCreateTables() &&
           Objects.equals(getJobsTable(), that.getJobsTable()) &&
           Objects.equals(getTablesTable(), that.getTablesTable()) &&
           Objects.equals(getSchemasTable(), that.getSchemasTable());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(isCreateTables(), getJobsTable(), getTablesTable(), getSchemasTable());
  }
}
