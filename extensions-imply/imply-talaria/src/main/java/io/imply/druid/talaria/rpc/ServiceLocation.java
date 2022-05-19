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
import org.apache.druid.server.DruidNode;

import java.util.Objects;

/**
 * Represents a service location at a particular point in time.
 */
public class ServiceLocation
{
  private final String host;
  private final int plaintextPort;
  private final int tlsPort;
  private final String basePath;

  public ServiceLocation(final String host, final int plaintextPort, final int tlsPort, final String basePath)
  {
    this.host = Preconditions.checkNotNull(host, "host");
    this.plaintextPort = plaintextPort;
    this.tlsPort = tlsPort;
    this.basePath = Preconditions.checkNotNull(basePath, "basePath");
  }

  public static ServiceLocation fromDruidNode(final DruidNode druidNode)
  {
    return new ServiceLocation(druidNode.getHost(), druidNode.getPlaintextPort(), druidNode.getTlsPort(), "");
  }

  public String getHost()
  {
    return host;
  }

  public int getPlaintextPort()
  {
    return plaintextPort;
  }

  public int getTlsPort()
  {
    return tlsPort;
  }

  public String getBasePath()
  {
    return basePath;
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
    ServiceLocation that = (ServiceLocation) o;
    return plaintextPort == that.plaintextPort
           && tlsPort == that.tlsPort
           && Objects.equals(host, that.host)
           && Objects.equals(basePath, that.basePath);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(host, plaintextPort, tlsPort, basePath);
  }

  @Override
  public String toString()
  {
    return "ServiceLocation{" +
           "host='" + host + '\'' +
           ", plaintextPort=" + plaintextPort +
           ", tlsPort=" + tlsPort +
           ", basePath='" + basePath + '\'' +
           '}';
  }
}
