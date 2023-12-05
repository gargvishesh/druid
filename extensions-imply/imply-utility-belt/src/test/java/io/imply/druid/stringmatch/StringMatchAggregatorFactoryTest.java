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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class StringMatchAggregatorFactoryTest
{
  private StringMatchAggregatorFactory factory;
  private ObjectMapper jsonMapper;

  @Before
  public void setUp()
  {
    factory = new StringMatchAggregatorFactory("foo", "bar", null, false);
    jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerSubtypes(StringMatchAggregatorFactory.class);
  }

  @Test
  public void test_combine_init_init()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null)
        )
    );
  }

  @Test
  public void test_combine_init_noMatch()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null)
        )
    );
  }

  @Test
  public void test_combine_noMatch_noMatch()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null)
        )
    );
  }

  @Test
  public void test_combine_foo_foo()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo")
        )
    );
  }

  @Test
  public void test_combine_foo_empty()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "")
        )
    );
  }

  @Test
  public void test_combine_empty_empty()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, ""),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, ""),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "")
        )
    );
  }

  @Test
  public void test_combine_foo_bar()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "bar")
        )
    );
  }

  @Test
  public void test_combine_foo_noMatch()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null)
        )
    );
  }

  @Test
  public void test_combine_noMatch_foo()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo")
        )
    );
  }

  @Test
  public void test_combine_foo_init()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null)
        )
    );
  }

  @Test
  public void test_combine_init_foo()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
        factory.combine(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo")
        )
    );
  }

  @Test
  public void test_finalize_init()
  {
    Assert.assertNull(
        factory.finalizeComputation(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null)
        )
    );
  }

  @Test
  public void test_finalize_noMatch()
  {
    Assert.assertNull(
        factory.finalizeComputation(
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null)
        )
    );
  }

  @Test
  public void test_finalize_foo()
  {
    Assert.assertEquals(
        "foo",
        factory.finalizeComputation(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo")
        )
    );
  }

  @Test
  public void test_getCacheKey()
  {
    final byte[] cacheKey1 = new StringMatchAggregatorFactory("a0", "x", null, false).getCacheKey();
    final byte[] cacheKey2 = new StringMatchAggregatorFactory("a0", "y", null, false).getCacheKey();
    final byte[] cacheKey3 = new StringMatchAggregatorFactory("a0", "y", null, true).getCacheKey();
    final byte[] cacheKey4 = new StringMatchAggregatorFactory(
        "a0",
        "y",
        StringMatchAggregatorFactory.DEFAULT_MAX_STRING_BYTES,
        false
    ).getCacheKey();

    Assert.assertFalse(Arrays.equals(cacheKey1, cacheKey2));
    Assert.assertFalse(Arrays.equals(cacheKey1, cacheKey3));
    Assert.assertFalse(Arrays.equals(cacheKey1, cacheKey4));
    Assert.assertFalse(Arrays.equals(cacheKey2, cacheKey3));
    Assert.assertArrayEquals(cacheKey2, cacheKey4);
    Assert.assertFalse(Arrays.equals(cacheKey3, cacheKey4));
  }

  @Test
  public void test_serde() throws IOException
  {
    Assert.assertEquals(factory, jsonMapper.readValue(jsonMapper.writeValueAsBytes(factory), AggregatorFactory.class));

    // Also try one with maxLength and combine set
    final StringMatchAggregatorFactory factoryWithExtras = new StringMatchAggregatorFactory("foo", "bar", 4, true);
    final AggregatorFactory factoryWithExtras2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(factoryWithExtras),
        AggregatorFactory.class
    );
    Assert.assertEquals(factoryWithExtras, factoryWithExtras2);
    Assert.assertEquals(4, (int) factoryWithExtras.getMaxStringBytes());
    Assert.assertTrue(factoryWithExtras.isCombine());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(StringMatchAggregatorFactory.class).usingGetClass().verify();
  }
}
