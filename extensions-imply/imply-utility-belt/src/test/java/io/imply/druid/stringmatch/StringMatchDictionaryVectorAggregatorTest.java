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

import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(Enclosed.class)
public class StringMatchDictionaryVectorAggregatorTest
{
  public abstract static class Base extends BaseStringMatchAggregatorTest
  {
    @Override
    protected SerializablePairLongString aggregateMultiValue(List<List<String>> data, int maxLength)
    {
      final StringMatchTestVectorSelector selector = new StringMatchTestVectorSelector(makeDictionary(data), true);
      final VectorAggregator aggregator = new StringMatchDictionaryVectorAggregator(selector, maxLength);
      final int capacity =
          new StringMatchAggregatorFactory("foo", "foo", maxLength, false).getMaxIntermediateSize() + 2;
      final ByteBuffer buf = ByteBuffer.allocate(capacity);
      buf.position(1);
      aggregator.init(buf, 1);

      final List<String> singlyValuedData = new ArrayList<>();
      for (List<String> row : data) {
        Assume.assumeTrue(row.size() == 1);
        singlyValuedData.add(row.get(0));
      }

      selector.setCurrentVector(singlyValuedData);
      aggregateVector(aggregator, buf, 1, data.size());

      final SerializablePairLongString retVal = (SerializablePairLongString) aggregator.get(buf, 1);

      Assert.assertEquals(buf.position(), 1);
      Assert.assertEquals(buf.limit(), capacity);
      Assert.assertEquals(0, buf.get(buf.limit() - 1));

      return retVal;
    }

    protected abstract void aggregateVector(VectorAggregator aggregator, ByteBuffer buf, int position, int numRows);
  }

  public static class RangeAggregateTest extends Base
  {
    @Override
    protected void aggregateVector(VectorAggregator aggregator, ByteBuffer buf, int position, int numRows)
    {
      aggregator.aggregate(buf, position, 0, numRows);
    }
  }

  public static class SlotAggregateTest extends Base
  {
    @Override
    protected void aggregateVector(VectorAggregator aggregator, ByteBuffer buf, int position, int numRows)
    {
      final int[] positions = new int[numRows];
      Arrays.fill(positions, position + 1);

      aggregator.aggregate(buf, numRows, positions, null, -1);
    }
  }

  public static class SlotWithRowsAggregateTest extends Base
  {
    @Override
    protected void aggregateVector(VectorAggregator aggregator, ByteBuffer buf, int position, int numRows)
    {
      final int[] positions = new int[numRows];
      Arrays.fill(positions, position + 1);

      final int[] rows = new int[numRows];
      for (int i = 0; i < numRows; i++) {
        rows[i] = i;
      }

      aggregator.aggregate(buf, numRows, positions, rows, -1);
    }
  }
}
