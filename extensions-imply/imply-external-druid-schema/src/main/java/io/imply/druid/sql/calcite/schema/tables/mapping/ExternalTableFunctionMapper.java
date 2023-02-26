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
 * Table function mapper is responsible for mapping the supplied
 * table function spec to the Polaris external table spec.
 */
public interface ExternalTableFunctionMapper
{
  PolarisExternalTableSpec getTableFunctionMapping(PolarisTableFunctionSpec serializedTableFnSpec);
}