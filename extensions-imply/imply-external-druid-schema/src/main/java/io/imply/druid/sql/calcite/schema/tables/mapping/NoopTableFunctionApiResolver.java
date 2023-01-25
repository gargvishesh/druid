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
 * No-op implementation for non-broker nodes.
 */
public class NoopTableFunctionApiResolver
    implements ExternalTableFunctionMapper {
  @Override
  public byte[] getTableFunctionMapping(byte[] serializedTableFnSpec) {
    // not supported for non-broker nodes.
    throw new UnsupportedOperationException("Non-broker nodes do not support tableSchemaUpdateListener");
  }
}
