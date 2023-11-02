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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseStringMatchAggregatorTest extends InitializedNullHandlingTest
{
  @Test
  public void test_empty()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
        aggregate(Collections.emptyList(), 100)
    );
  }

  @Test
  public void test_allNull()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
        aggregate(Collections.singletonList(null), 100)
    );
  }

  @Test
  public void test_singleValue()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
        aggregate(Arrays.asList("foo", "foo"), 100)
    );
  }

  @Test
  public void test_multiValue()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        aggregateMultiValue(Collections.singletonList(Arrays.asList("foo", "foo")), 100)
    );
  }

  @Test
  public void test_singleValue_chop()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "fo"),
        aggregate(Arrays.asList("foo", "foo"), 2)
    );
  }

  @Test
  public void test_singleValue_chop_nonAscii()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "ƒo"),
        aggregate(Arrays.asList("ƒoo", "ƒoo"), 3)
    );
  }

  @Test
  public void test_singleValueWithNull()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
        aggregate(Arrays.asList(null, "foo", null, "foo"), 100)
    );
  }

  @Test
  public void test_singleValueWithEmptyLists()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
        aggregateMultiValue(
            Arrays.asList(
                Collections.emptyList(),
                Collections.singletonList("foo"),
                Collections.emptyList(),
                Collections.singletonList("foo")
            ),
            100
        )
    );
  }

  @Test
  public void test_singleValueWithMultiValues()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        aggregateMultiValue(
            Arrays.asList(
                Collections.singletonList("foo"),
                Arrays.asList("foo", "foo"),
                Collections.singletonList("foo")
            ),
            100
        )
    );
  }

  @Test
  public void test_singleValueWithEmptyString()
  {
    Assert.assertEquals(
        new SerializablePairLongString(
            (long) StringMatchUtil.NOT_MATCHING,
            NullHandling.sqlCompatible() ? null : "foo"
        ),
        aggregate(Arrays.asList("", "foo", "", "foo"), 100)
    );
  }

  @Test
  public void test_variousValues()
  {
    Assert.assertEquals(
        new SerializablePairLongString(
            (long) StringMatchUtil.NOT_MATCHING,
            NullHandling.sqlCompatible() ? null : "foo"
        ),
        aggregate(Arrays.asList("bar", "foo", "baz"), 100)
    );
  }

  @Test
  public void test_variousMultiValues()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        aggregateMultiValue(
            Arrays.asList(
                Arrays.asList("foo", "bar"),
                Arrays.asList("bar", "baz")
            ),
            100
        )
    );
  }

  private SerializablePairLongString aggregate(final List<String> data, final int maxLength)
  {
    return aggregateMultiValue(
        data.stream().map(Collections::singletonList).collect(Collectors.toList()),
        maxLength
    );
  }

  protected abstract SerializablePairLongString aggregateMultiValue(final List<List<String>> data, final int maxLength);
}
