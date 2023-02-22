/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.catalog.model.table.FormattedInputSourceDefn;
import org.apache.druid.catalog.model.table.ResolvedExternalTable;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Base class for Polaris resolved input sources that require an input format.
 * By default, an input source supports all formats defined in the table registry, but
 * specific input sources can be more restrictive. The list of formats defines the list
 * of SQL function arguments available when defining a table from scratch.
 */
public abstract class BasePolarisInputSourceDefn extends FormattedInputSourceDefn
{
  private final PolarisTableFunctionResolver resolver;

  public BasePolarisInputSourceDefn(
      final PolarisTableFunctionResolver resolver
  )
  {
    this.resolver = resolver;
  }

  /**
   * Convert the input source using arguments to a "from scratch" table function.
   */
  @Override
  @Nullable
  protected InputSource convertArgsToSource(Map<String, Object> args, ObjectMapper jsonMapper)
  {
    final PolarisTableFunctionSpec tblFnSpec = convertArgsToTblFnDefn(args);
    final PolarisExternalTableSpec polarisExtTblSpec = resolver.resolve(tblFnSpec);
    return polarisExtTblSpec.getInputSource();
  }

  protected abstract PolarisTableFunctionSpec convertArgsToTblFnDefn(Map<String, Object> args);

  @Override
  protected void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args)
  {

  }

  @Override
  protected ExternalTableSpec convertCompletedTable(
      ResolvedExternalTable table,
      Map<String, Object> args,
      List<ColumnSpec> columns
  )
  {
    throw new UOE(StringUtils.format("Input source type [%s] does not allow for partial tables", typeValue()));
  }

  @Override
  protected abstract List<TableFunction.ParameterDefn> adHocTableFnParameters();

  @Override
  public abstract String typeValue();

  @Override
  public TableFunction partialTableFn(ResolvedExternalTable table)
  {
    throw new UOE(StringUtils.format("Input source type [%s] does not allow for partial tables", typeValue()));
  }
}
