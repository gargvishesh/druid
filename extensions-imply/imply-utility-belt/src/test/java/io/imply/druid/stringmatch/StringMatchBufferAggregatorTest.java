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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.DimensionSelector;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeSet;

public class StringMatchBufferAggregatorTest extends BaseStringMatchAggregatorTest
{
  @Override
  protected SerializablePairLongString aggregateMultiValue(final List<List<String>> data, final int maxLength)
  {
    final TreeSet<String> dictionarySet = Sets.newTreeSet(Comparators.naturalNullsFirst());
    for (List<String> row : data) {
      dictionarySet.addAll(row);
    }
    final List<String> dictionary = Lists.newArrayList(dictionarySet);
    final StringMatchTestDimensionSelector selector = new StringMatchTestDimensionSelector(dictionary);
    final BufferAggregator aggregator = makeAggregator(selector, maxLength);
    final int capacity = new StringMatchAggregatorFactory("foo", "foo", maxLength).getMaxIntermediateSize() + 2;
    final ByteBuffer buf = ByteBuffer.allocate(capacity);
    buf.position(1);
    aggregator.init(buf, 1);

    for (final List<String> row : data) {
      selector.setCurrentRow(row);
      aggregator.aggregate(buf, 1);
    }

    final SerializablePairLongString retVal = (SerializablePairLongString) aggregator.get(buf, 1);

    Assert.assertEquals(buf.position(), 1);
    Assert.assertEquals(buf.limit(), capacity);
    Assert.assertEquals(0, buf.get(buf.limit() - 1));

    return retVal;
  }

  protected BufferAggregator makeAggregator(final DimensionSelector selector, final int maxLength)
  {
    return new StringMatchBufferAggregator(selector, maxLength);
  }
}
