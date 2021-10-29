/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

public class SqlAsyncMetadataStorageTableConfig
{
  private static final String DEFAULT_BASE = "druid";
  private static final String DEFAULT_SQL_ASYNC_QUERIES_TABLE = "sqlAsyncQueries";

  @JsonProperty("base")
  private final String base;

  @JsonProperty("sqlAsyncQueries")
  private final String sqlAsyncQueriesTable;

  @JsonCreator
  public SqlAsyncMetadataStorageTableConfig(
      @JsonProperty("base") @Nullable String base,
      @JsonProperty("sqlAsyncQueries") @Nullable String sqlAsyncQueriesTable
  )
  {
    this.base = (base == null) ? DEFAULT_BASE : base;
    this.sqlAsyncQueriesTable = makeTableName(sqlAsyncQueriesTable, DEFAULT_SQL_ASYNC_QUERIES_TABLE);
  }

  // Copied from MetadataStorageTablesConfig
  private String makeTableName(String explicitTableName, String defaultSuffix)
  {
    if (explicitTableName == null) {
      return StringUtils.format("%s_%s", base, defaultSuffix);
    }

    return explicitTableName;
  }

  public String getSqlAsyncQueriesTable()
  {
    return sqlAsyncQueriesTable;
  }
}
