/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.mapping;

import io.imply.druid.sql.calcite.external.PolarisExternalTableSpec;
import io.imply.druid.sql.calcite.external.PolarisTableFunctionSpec;

/**
 * No-op implementation for non-broker nodes.
 */
public class NoopTableFunctionApiResolver
    implements ExternalTableFunctionMapper
{
  @Override
  public PolarisExternalTableSpec getTableFunctionMapping(PolarisTableFunctionSpec serializedTableFnSpec)
  {
    // not supported for non-broker nodes.
    throw new UnsupportedOperationException("Non-broker nodes do not support getTableFunctionMapping");
  }
}
