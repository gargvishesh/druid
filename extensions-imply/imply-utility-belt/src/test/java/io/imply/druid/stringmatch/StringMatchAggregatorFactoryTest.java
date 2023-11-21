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

public class StringMatchAggregatorFactoryTest
{
  private StringMatchAggregatorFactory factory;
  private ObjectMapper jsonMapper;

  @Before
  public void setUp()
  {
    factory = new StringMatchAggregatorFactory("foo", "bar", null);
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
  public void testSerde() throws IOException
  {
    Assert.assertEquals(factory, jsonMapper.readValue(jsonMapper.writeValueAsBytes(factory), AggregatorFactory.class));

    // Also try one with maxLength set
    final StringMatchAggregatorFactory factoryWithMaxLength = new StringMatchAggregatorFactory("foo", "bar", 4);
    final AggregatorFactory factoryWithMaxLength2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(factoryWithMaxLength),
        AggregatorFactory.class
    );
    Assert.assertEquals(factoryWithMaxLength, factoryWithMaxLength2);
    Assert.assertEquals(4, (int) factoryWithMaxLength.getMaxStringBytes());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(StringMatchAggregatorFactory.class).usingGetClass().verify();
  }
}
