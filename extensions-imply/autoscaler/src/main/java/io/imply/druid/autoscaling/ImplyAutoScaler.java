/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.imply.druid.autoscaling.client.ImplyManagerServiceClient;
import io.imply.druid.autoscaling.server.ListInstancesResponse;
import io.imply.druid.autoscaling.server.ProvisionInstancesRequest;
import io.imply.druid.autoscaling.server.ProvisionInstancesResponse;
import org.apache.druid.indexing.overlord.autoscaling.AutoScaler;
import org.apache.druid.indexing.overlord.autoscaling.AutoScalingData;
import org.apache.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningConfig;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This module permits the autoscaling of the workers in Imply Manager
 *
 * General notes:
 * - The IPs are IPs as in Internet Protocol, and they look like 1.2.3.4
 * - The IDs are the names of the instances of instances created, they look like
 */
@JsonTypeName("imply")
public class ImplyAutoScaler implements AutoScaler<ImplyEnvironmentConfig>
{
  private static final EmittingLogger log = new EmittingLogger(ImplyAutoScaler.class);

  private final ImplyEnvironmentConfig envConfig;
  private final int minNumWorkers;
  private final int maxNumWorkers;
  private final ImplyManagerServiceClient implyManagerServiceClient;
  private final SimpleWorkerProvisioningConfig config;

  @JsonCreator
  public ImplyAutoScaler(
      @JsonProperty("minNumWorkers") int minNumWorkers,
      @JsonProperty("maxNumWorkers") int maxNumWorkers,
      @JsonProperty("envConfig") ImplyEnvironmentConfig envConfig,
      @JacksonInject ImplyManagerServiceClient implyManagerServiceClient,
      @JacksonInject SimpleWorkerProvisioningConfig config
  )
  {
    Preconditions.checkArgument(minNumWorkers > 0,
                                "minNumWorkers must be greater than 0");
    this.minNumWorkers = minNumWorkers;
    Preconditions.checkArgument(maxNumWorkers > 0,
                                "maxNumWorkers must be greater than 0");
    Preconditions.checkArgument(maxNumWorkers > minNumWorkers,
                                "maxNumWorkers must be greater than minNumWorkers");
    this.maxNumWorkers = maxNumWorkers;
    this.envConfig = envConfig;
    this.implyManagerServiceClient = implyManagerServiceClient;
    this.config = config;
  }

  @Override
  @JsonProperty
  public int getMinNumWorkers()
  {
    return minNumWorkers;
  }

  @Override
  @JsonProperty
  public int getMaxNumWorkers()
  {
    return maxNumWorkers;
  }

  @Override
  @JsonProperty
  public ImplyEnvironmentConfig getEnvConfig()
  {
    return envConfig;
  }

  @Override
  public AutoScalingData provision()
  {
    try {
      List<String> before = getRunningInstances();
      log.debug("Existing instances [%s]", String.join(",", before));

      int toSize = Math.min(before.size() + 1, getMaxNumWorkers());
      if (before.size() >= toSize) {
        // nothing to scale
        return new AutoScalingData(new ArrayList<>());
      }
      log.info("Asked to provision instances, will resize to %d", toSize);

      if (config.getWorkerVersion() == null) {
        log.error("Unable to provision new instance as worker version is not configured");
        return new AutoScalingData(new ArrayList<>());
      }
      // TODO: total size or delta?
      ProvisionInstancesRequest request = new ProvisionInstancesRequest(ImmutableList.of(new ProvisionInstancesRequest.Instance(config.getWorkerVersion(), 1)));
      ProvisionInstancesResponse response = implyManagerServiceClient.provisionInstances(envConfig, request);
      return new AutoScalingData(response.getInstanceIds());
    }
    catch (Exception e) {
      log.error(e, "Unable to provision any gce instances.");
    }
    return new AutoScalingData(new ArrayList<>());
  }

  /**
   * Terminates the instances in the list of IPs provided by the caller
   */
  @Override
  public AutoScalingData terminate(List<String> ips)
  {
    log.info("Asked to terminate: [%s]", String.join(",", ips));

    if (ips.isEmpty()) {
      return new AutoScalingData(new ArrayList<>());
    }

    List<String> nodeIds = ipToIdLookup(ips);
    try {
      return terminateWithIds(nodeIds != null ? nodeIds : new ArrayList<>());
    }
    catch (Exception e) {
      log.error(e, "Unable to terminate instances.");
    }

    return new AutoScalingData(new ArrayList<>());
  }

  /**
   * Terminates the instances in the list of IDs provided by the caller
   */
  @Override
  public AutoScalingData terminateWithIds(List<String> ids)
  {
    log.info("Asked to terminate IDs: [%s]", String.join(",", ids));

    if (ids.isEmpty()) {
      return new AutoScalingData(new ArrayList<>());
    }
    for (String id : ids) {
      try {
        implyManagerServiceClient.terminateInstances(envConfig, id);
      }
      catch (Exception e) {
        log.error(e, "Unable to terminate instances: [%s]", id);
      }
    }
    return new AutoScalingData(ids);
  }

  // Returns the list of the IDs of the machines running in the MIG
  private List<String> getRunningInstances()
  {
    ArrayList<String> ids = new ArrayList<>();
    try {
      ListInstancesResponse response = implyManagerServiceClient.listInstances(envConfig, ImmutableList.of());
      // todo filter on status?
      ids.addAll(response.getInstances().stream().map(instance -> instance.getId()).collect(Collectors.toList()));
      log.debug("Found running instances [%s]", String.join(",", ids));
    }
    catch (Exception e) {
      log.error(e, "Unable to get instances.");
    }
    return ids;
  }

  /**
   * Converts the IPs to IDs
   */
  @Override
  public List<String> ipToIdLookup(List<String> ips)
  {
    log.info("Asked IPs -> IDs for: [%s]", String.join(",", ips));

    if (ips.isEmpty()) {
      return new ArrayList<>();
    }

    ArrayList<String> ids = new ArrayList<>();
    try {
      ListInstancesResponse response = implyManagerServiceClient.listInstances(envConfig, ImmutableList.of());
      // todo filter on status?
      ids.addAll(response.getInstances().stream().filter(instance -> ips.contains(instance.getIp())).map(instance -> instance.getId()).collect(Collectors.toList()));
    }
    catch (Exception e) {
      log.error(e, "Unable to get instances.");
    }
    return ids;
  }

  /**
   * Converts the IDs to IPs
   */
  @Override
  public List<String> idToIpLookup(List<String> nodeIds)
  {
    log.info("Asked IDs -> IPs for: [%s]", String.join(",", nodeIds));

    if (nodeIds.isEmpty()) {
      return new ArrayList<>();
    }

    ArrayList<String> ips = new ArrayList<>();
    try {
      ListInstancesResponse response = implyManagerServiceClient.listInstances(envConfig, ImmutableList.of());
      // todo filter on status?
      ips.addAll(response.getInstances().stream().filter(instance -> nodeIds.contains(instance.getId())).map(instance -> instance.getIp()).collect(Collectors.toList()));
    }
    catch (Exception e) {
      log.error(e, "Unable to get instances.");
    }
    return ips;
  }


}
