/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.state.cache;

import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;

import javax.annotation.Nullable;
import java.util.Map;

public class DefaultExternalDruidSchemaCacheManager implements ExternalDruidSchemaCacheManager
{
  @Override
  public void updateTableSchemas(byte[] serializedSchemaMap)
  {

  }

  @Nullable
  @Override
  public Map<String, TableSchema> getTableSchemas()
  {
    return null;
  }
}
