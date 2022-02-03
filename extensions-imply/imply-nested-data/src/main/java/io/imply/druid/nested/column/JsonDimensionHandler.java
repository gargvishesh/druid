/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.segment.DimensionIndexer;

public class JsonDimensionHandler extends NestedDataDimensionHandler
{
  public JsonDimensionHandler(String name)
  {
    super(name);
  }

  @Override
  public DimensionIndexer<StructuredData, StructuredData, StructuredData> makeIndexer(boolean useMaxMemoryEstimates)
  {
    return new JsonColumnIndexer();
  }
}
