/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.google.inject.Inject;
import io.imply.druid.sql.calcite.schema.tables.mapping.ExternalTableFunctionMapper;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;

public class PolarisTableFunctionResolverImpl implements PolarisTableFunctionResolver
{
  private final ExternalTableFunctionMapper tableFunctionMapper;

  @Inject
  public PolarisTableFunctionResolverImpl(
      final ExternalTableFunctionMapper tableFunctionMapper
  )
  {
    this.tableFunctionMapper = tableFunctionMapper;
  }

  @Override
  @Nullable
  public PolarisExternalTableSpec resolve(@Nullable final PolarisTableFunctionSpec fn)
  {
    if (null == fn) {
      throw new IAE("Polaris table function spec must be provided.");
    }
    return tableFunctionMapper.getTableFunctionMapping(fn);
  }
}
