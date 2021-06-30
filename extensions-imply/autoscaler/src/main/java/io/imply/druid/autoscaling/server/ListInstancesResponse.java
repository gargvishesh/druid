package io.imply.druid.autoscaling.server;

import com.sun.istack.internal.Nullable;

import java.util.List;
import java.util.Objects;

public class ListInstancesResponse
{
  private final List<Instance> instances;

  @JsonCreator
  public ProvisionInstancesRequest(
      @JsonProperty("instances") List<Instance> instances
  )
  {
    this.instances = instances;
  }

  @JsonProperty
  public List<Instance> getInstances()
  {
    return instances;
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
    ListInstancesResponse that = (ListInstancesResponse) o;
    return Objects.equals(instances, that.instances);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(instances);
  }

  public static class Instance
  {
    private final String status;
    private final String ip;
    private final String id;

    @JsonCreator
    public Instance(
        @JsonProperty("status") String status,
        @JsonProperty("ip") String ip,
        @JsonProperty("id") String id
    )
    {
      this.status = status;
      this.ip = ip;
      this.id = id;
    }

    @JsonProperty("status")
    public String getStatus()
    {
      return status;
    }

    @JsonProperty("ip")
    public String getIp()
    {
      return ip;
    }

    @JsonProperty("id")
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
}
