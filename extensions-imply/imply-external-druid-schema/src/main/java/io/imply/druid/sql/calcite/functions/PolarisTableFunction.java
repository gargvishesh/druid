/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */


package io.imply.druid.sql.calcite.functions;

import org.apache.druid.catalog.model.table.BaseTableFunction;

import java.util.List;
import java.util.Map;

/**
 * Table functions that are resolved by Polaris. Such table functions may have
 * arguments that need to be resolved / converted by Polaris with information
 * augmented there to be usable by Druid. Necessarily, such functions will
 * require making an RPC call to Polaris via the {@link PolarisTableFunctionResolver}.
 */
public abstract class PolarisTableFunction extends BaseTableFunction
{
  protected final PolarisTableFunctionResolver resolver;

  public PolarisTableFunction(final PolarisTableFunctionResolver resolver, List<ParameterDefn> parameters)
  {
    super(parameters);
    this.resolver = resolver;
  }

  /**
   * Converts the function arguments to an object that holds the name of the function
   * and the parameters of the function and their value.
   *
   * @param args A map representing the arguments to the function.
   * @return An object holding information about the function including its name and arguments.
   */
  public abstract PolarisTableFunctionSpec convertArgsToTblFnDefn(Map<String, Object> args);

  /**
   * @return the name of the function
   */
  public abstract String name();

  /**
   * @return The error to return if no columns are specified.
   */
  public abstract String columnsDefnUnspecifiedError();
  /**
   * @return The error to return if the columns are defined more than once.
   */
  public abstract String columnsDefnCollisionErrorStr();
}
