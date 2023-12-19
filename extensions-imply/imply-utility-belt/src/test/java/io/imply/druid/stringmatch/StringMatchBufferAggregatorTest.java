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

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.DimensionSelector;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.util.List;

public class StringMatchBufferAggregatorTest extends BaseStringMatchAggregatorTest
{
  @Override
  protected SerializablePairLongString aggregateMultiValue(final List<List<String>> data, final int maxLength)
  {
    final StringMatchTestDimensionSelector selector = new StringMatchTestDimensionSelector(makeDictionary(data));
    final BufferAggregator aggregator = makeAggregator(selector, maxLength);
    final int capacity = new StringMatchAggregatorFactory("foo", "foo", maxLength, false).getMaxIntermediateSize() + 3;
    final ByteBuffer buf = ByteBuffer.allocate(capacity);
    buf.position(capacity);
    aggregator.init(buf, 2);

    for (final List<String> row : data) {
      selector.setCurrentRow(row);
      aggregator.aggregate(buf, 2);
    }

    final SerializablePairLongString retVal = (SerializablePairLongString) aggregator.get(buf, 2);

    Assert.assertEquals(buf.position(), capacity);
    Assert.assertEquals(buf.limit(), capacity);
    Assert.assertEquals(0, buf.get(0));
    Assert.assertEquals(0, buf.get(buf.limit() - 1));

    return retVal;
  }

  protected BufferAggregator makeAggregator(final DimensionSelector selector, final int maxLength)
  {
    return new StringMatchBufferAggregator(selector, maxLength);
  }
}