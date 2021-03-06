/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class ViewStateSqlMetadataConnectorTableConfig
{
  @JsonProperty("createTables")
  private final boolean createTables;

  @JsonProperty("views")
  private final String viewsTable;

  @JsonCreator
  public ViewStateSqlMetadataConnectorTableConfig(
      @JsonProperty("createTables") @Nullable Boolean createTables,
      @JsonProperty("views") String viewsTable
  )
  {
    this.createTables = createTables == null ? true : createTables;
    this.viewsTable = viewsTable == null ? "druid_views" : viewsTable;
  }

  public boolean isCreateTables()
  {
    return createTables;
  }

  public String getViewsTable()
  {
    return viewsTable;
  }
}
