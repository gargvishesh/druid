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
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public abstract class BaseStringMatchCombiningAggregatorTest extends InitializedNullHandlingTest
{
  @Test
  public void test_init_init()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null)
        )
    );
  }

  @Test
  public void test_init_noMatch()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null)
        )
    );
  }

  @Test
  public void test_noMatch_noMatch()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null)
        )
    );
  }

  @Test
  public void test_foo_foo()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo")
        )
    );
  }

  @Test
  public void test_foo_bar()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "bar")
        )
    );
  }

  @Test
  public void test_foo_empty()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "")
        )
    );
  }

  @Test
  public void test_empty_empty()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, ""),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, ""),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "")
        )
    );
  }

  @Test
  public void test_foo_noMatch()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null)
        )
    );
  }

  @Test
  public void test_noMatch_foo()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo")
        )
    );
  }

  @Test
  public void test_foo_init()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null)
        )
    );
  }

  @Test
  public void test_init_foo()
  {
    Assert.assertEquals(
        new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo"),
        aggregate(
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, null),
            new SerializablePairLongString((long) StringMatchUtil.MATCHING, "foo")
        )
    );
  }

  protected abstract SerializablePairLongString aggregate(final SerializablePairLongString... values);
}
