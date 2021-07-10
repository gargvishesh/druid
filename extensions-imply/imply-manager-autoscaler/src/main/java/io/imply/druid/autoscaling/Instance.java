/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling;

import org.apache.druid.java.util.common.logger.Logger;

import java.util.Objects;

public class Instance
{
  private static final Logger LOGGER = new Logger(Instance.class);

  public enum Status
  {
    STARTING,
    RUNNING,
    TERMINATING,
    UNKNOWN
  }

  // status of the Druid instance on Imply Manager
  private final Status status;
  // ip of the Druid instance
  private final String ip;
  // id of the Druid instance assigned by Imply Manager
  private final String id;

  public Instance(
      Status status,
      String ip,
      String id
  )
  {
    this.status = status;
    this.ip = ip;
    this.id = id;
  }

  public Instance(
      String status,
      String ip,
      String id
  )
  {
    Status statusEnum = Status.UNKNOWN;
    try {
      statusEnum = Status.valueOf(status);
    }
    catch (IllegalArgumentException e) {
      LOGGER.warn("Got invalid instance status of status: %s for ip: %s, id: %s", status, ip, id);
    }
    this.status = statusEnum;
    this.ip = ip;
    this.id = id;
  }

  public Status getStatus()
  {
    return status;
  }

  public String getIp()
  {
    return ip;
  }

  public String getId()
  {
    return id;
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
    Instance instance = (Instance) o;
    return Objects.equals(status, instance.status) &&
           Objects.equals(ip, instance.ip) &&
           Objects.equals(id, instance.id);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(status, ip, id);
  }
}
