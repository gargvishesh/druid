/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ServiceLocationsTest
{
  @Test
  public void test_forLocation()
  {
    final ServiceLocation location = new ServiceLocation("h", -1, 2, "");
    final ServiceLocations locations = ServiceLocations.forLocation(location);

    Assert.assertEquals(ImmutableSet.of(location), locations.getLocations());
    Assert.assertFalse(locations.isClosed());
  }

  @Test
  public void test_forLocations()
  {
    final ServiceLocation location1 = new ServiceLocation("h", -1, 2, "");
    final ServiceLocation location2 = new ServiceLocation("h", -1, 2, "");

    final ServiceLocations locations = ServiceLocations.forLocations(ImmutableSet.of(location1, location2));

    Assert.assertEquals(ImmutableSet.of(location1, location2), locations.getLocations());
    Assert.assertFalse(locations.isClosed());
  }

  @Test
  public void test_closed()
  {
    final ServiceLocations locations = ServiceLocations.closed();

    Assert.assertEquals(Collections.emptySet(), locations.getLocations());
    Assert.assertTrue(locations.isClosed());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ServiceLocations.class)
                  .usingGetClass()
                  .verify();
  }
}
