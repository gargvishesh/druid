/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Returned by {@link ServiceLocator#locate()}. See that function for documentation.
 */
public class ServiceLocations
{
  private final Set<ServiceLocation> locations;
  private final boolean closed;

  private ServiceLocations(final Set<ServiceLocation> locations, final boolean closed)
  {
    this.locations = Preconditions.checkNotNull(locations, "locations");
    this.closed = closed;

    if (closed && !locations.isEmpty()) {
      throw new IAE("Locations must be empty for closed services");
    }
  }

  public static ServiceLocations forLocation(final ServiceLocation location)
  {
    return new ServiceLocations(Collections.singleton(Preconditions.checkNotNull(location)), false);
  }

  public static ServiceLocations forLocations(final Set<ServiceLocation> locations)
  {
    return new ServiceLocations(locations, false);
  }

  public static ServiceLocations closed()
  {
    return new ServiceLocations(Collections.emptySet(), true);
  }

  public Set<ServiceLocation> getLocations()
  {
    return locations;
  }

  public boolean isClosed()
  {
    return closed;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServiceLocations that = (ServiceLocations) o;
    return closed == that.closed && Objects.equals(locations, that.locations);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(locations, closed);
  }

  @Override
  public String toString()
  {
    return "ServiceLocations{" +
           "locations=" + locations +
           ", closed=" + closed +
           '}';
  }
}
