/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import org.junit.Assert;
import org.junit.Test;

public class ClusterByKeyTest
{
  @Test
  public void testEqualsAndHashCode()
  {
    // Can't use EqualsVerifier because it requires that cached hashcodes be computed in the constructor.
    Assert.assertEquals(ClusterByKey.of(1L), ClusterByKey.of(1L));
    Assert.assertNotEquals(ClusterByKey.of(1L), ClusterByKey.of(2L));
    Assert.assertEquals(ClusterByKey.of(1L, "abc"), ClusterByKey.of(1L, "abc"));
    Assert.assertNotEquals(ClusterByKey.of(1L, "abc"), ClusterByKey.of(1L, "def"));

    Assert.assertEquals(ClusterByKey.of(1L).hashCode(), ClusterByKey.of(1L).hashCode());
    Assert.assertNotEquals(ClusterByKey.of(1L).hashCode(), ClusterByKey.of(2L).hashCode());
    Assert.assertEquals(ClusterByKey.of(1L, "abc").hashCode(), ClusterByKey.of(1L, "abc").hashCode());
    Assert.assertNotEquals(ClusterByKey.of(1L, "abc").hashCode(), ClusterByKey.of(1L, "def").hashCode());
  }
}
