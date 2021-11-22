/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.state.notifier;

import java.util.function.Supplier;

public class NoopExternalDruidSchemaCacheNotifier implements ExternalDruidSchemaCacheNotifier
{
  @Override
  public void setSchemaUpdateSource(Supplier<byte[]> updateSource)
  {
    throw new UnsupportedOperationException("Non-coordinater does not support setSchemaUpdateSource");
  }

  @Override
  public void scheduleSchemaUpdate()
  {
    throw new UnsupportedOperationException("Non-coordinator does not support scheduleSchemaUpdate");
  }
}
