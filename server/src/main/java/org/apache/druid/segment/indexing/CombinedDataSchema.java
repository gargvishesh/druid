/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.indexing;

import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Class representing the combined DataSchema of a set of segments, currently used only by Compaction.
 */
public class CombinedDataSchema extends DataSchema
{
  private final MultiValuedColumnsInfo multiValuedColumnsInfo;
  private static final MultiValuedColumnsInfo NOT_PROCESSED = new MultiValuedColumnsInfo(false, Collections.emptySet());

  public CombinedDataSchema(
      String dataSource,
      @Nullable TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      AggregatorFactory[] aggregators,
      GranularitySpec granularitySpec,
      TransformSpec transformSpec,
      MultiValuedColumnsInfo multiValuedColumnsInfo
  )
  {
    super(
        dataSource,
        timestampSpec,
        dimensionsSpec,
        aggregators,
        granularitySpec,
        transformSpec,
        null,
        null
    );
    this.multiValuedColumnsInfo = multiValuedColumnsInfo;
  }

  @Nonnull
  public MultiValuedColumnsInfo getMultiValuedColumnsInfo()
  {
    return multiValuedColumnsInfo;
  }


  public static class MultiValuedColumnsInfo
  {
    final boolean processed;
    final Set<String> multiValuedColumns;

    public MultiValuedColumnsInfo(boolean processed, Set<String> multiValuedColumns)
    {
      this.processed = processed;
      this.multiValuedColumns = multiValuedColumns;
    }

    public static MultiValuedColumnsInfo notProcessed()
    {
      return NOT_PROCESSED;
    }

    public static MultiValuedColumnsInfo processed()
    {
      return new MultiValuedColumnsInfo(true, new HashSet<>());
    }

    public void addMultiValuedColumn(String col)
    {
      multiValuedColumns.add(col);
    }

    public boolean isProcessed()
    {
      return processed;
    }

    @Nonnull
    public Set<String> getMultiValuedColumns()
    {
      return multiValuedColumns;
    }
  }
}
