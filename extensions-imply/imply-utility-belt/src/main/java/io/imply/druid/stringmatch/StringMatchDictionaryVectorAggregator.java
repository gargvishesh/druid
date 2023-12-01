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

package io.imply.druid.stringmatch;

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringMatchDictionaryVectorAggregator implements VectorAggregator
{
  private final SingleValueDimensionVectorSelector selector;
  private final int maxLength;
  private final int nullId;

  public StringMatchDictionaryVectorAggregator(SingleValueDimensionVectorSelector selector, int maxLength)
  {
    this.selector = selector;
    this.maxLength = maxLength;
    this.nullId = StringMatchUtil.getNullId(selector);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    StringMatchUtil.initDictionary(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    if (!StringMatchUtil.isMatching(buf, position)) {
      return;
    }

    final int[] rowVector = selector.getRowVector();

    for (int i = startRow; i < endRow; i++) {
      final int id = rowVector[i];
      StringMatchUtil.accumulateDictionaryId(selector, buf, position, id, nullId);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    final int[] rowVector = selector.getRowVector();

    for (int i = 0; i < numRows; i++) {
      final int position = positions[i] + positionOffset;
      if (StringMatchUtil.isMatching(buf, position)) {
        final int id = rowVector[rows != null ? rows[i] : i];
        StringMatchUtil.accumulateDictionaryId(selector, buf, position, id, nullId);
      }
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return StringMatchUtil.getResultWithDictionary(buf, position, selector, maxLength);
  }

  @Override
  public void close()
  {

  }
}
