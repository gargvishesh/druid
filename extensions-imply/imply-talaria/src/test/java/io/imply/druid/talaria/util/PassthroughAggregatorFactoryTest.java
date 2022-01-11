/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.junit.Assert;
import org.junit.Test;

public class PassthroughAggregatorFactoryTest
{
  @Test
  public void testRequiredFields()
  {
    Assert.assertEquals(
        ImmutableList.of("x"),
        new PassthroughAggregatorFactory("x", "y").requiredFields()
    );
  }

  @Test
  public void testGetCombiningFactory()
  {
    Assert.assertEquals(
        new PassthroughAggregatorFactory("x", "y"),
        new PassthroughAggregatorFactory("x", "y").getCombiningFactory()
    );
  }

  @Test
  public void testGetMergingFactoryOk() throws AggregatorFactoryNotMergeableException
  {
    final AggregatorFactory mergingFactory =
        new PassthroughAggregatorFactory("x", "y").getMergingFactory(new PassthroughAggregatorFactory("x", "y"));

    Assert.assertEquals(new PassthroughAggregatorFactory("x", "y"), mergingFactory);
  }

  @Test
  public void testGetMergingFactoryNotOk()
  {
    Assert.assertThrows(
        AggregatorFactoryNotMergeableException.class,
        () -> new PassthroughAggregatorFactory("x", "y").getMergingFactory(new PassthroughAggregatorFactory("x", "z"))
    );

    Assert.assertThrows(
        AggregatorFactoryNotMergeableException.class,
        () -> new PassthroughAggregatorFactory("x", "y").getMergingFactory(new PassthroughAggregatorFactory("z", "y"))
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(PassthroughAggregatorFactory.class).usingGetClass().verify();
  }
}
