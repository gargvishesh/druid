/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */


package io.imply.druid.sql.calcite.external;

import org.apache.druid.catalog.model.table.ExternalTableSpec;

/**
 * Handles making RPC call to Polaris to resolve table functions
 */
public interface PolarisTableFunctionResolver
{
  /**
   * Resolves the table function
   * @param fn The table function to resolve
   * @return The {@link ExternalTableSpec} that the function resolves to.
   */
  PolarisExternalTableSpec resolve(PolarisTableFunctionSpec fn);
}
