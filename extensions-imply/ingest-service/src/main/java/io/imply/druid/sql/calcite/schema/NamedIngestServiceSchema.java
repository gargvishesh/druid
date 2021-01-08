/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema;

import com.google.inject.Inject;
import org.apache.calcite.schema.Schema;
import org.apache.druid.sql.calcite.schema.NamedSchema;

public class NamedIngestServiceSchema implements NamedSchema
{
  private static final String NAME = "imply";

  private final IngestServiceSchema ingestServiceSchema;

  @Inject
  NamedIngestServiceSchema(IngestServiceSchema ingestServiceSchema)
  {
    this.ingestServiceSchema = ingestServiceSchema;
  }

  @Override
  public String getSchemaName()
  {
    return NAME;
  }

  @Override
  public Schema getSchema()
  {
    return ingestServiceSchema;
  }
}
