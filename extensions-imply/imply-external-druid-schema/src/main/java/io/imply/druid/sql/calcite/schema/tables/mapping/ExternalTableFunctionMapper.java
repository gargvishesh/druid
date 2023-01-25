/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.mapping;


/**
 * Table function mapper is responsible for mapping the supplied
 * table function spec to the serialized Polaris external table spec.
 */
public interface ExternalTableFunctionMapper
{
  byte[] getTableFunctionMapping(byte[] serializedTableFnSpec);
}
