package io.imply.druid.sql.calcite.functions;

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
  ExternalTableSpec resolve(PolarisTableFunctionDefn fn);
}
