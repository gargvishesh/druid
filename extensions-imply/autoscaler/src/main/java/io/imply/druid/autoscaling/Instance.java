package io.imply.druid.autoscaling;

import org.apache.druid.java.util.common.logger.Logger;

import java.util.Objects;

public class Instance
{
  private static final Logger LOGGER = new Logger(Instance.class);

  public enum Status {
    STARTING,
    RUNNING,
    TERMINATING,
    UNKNOWN
  }

  private final Status status;
  private final String ip;
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
