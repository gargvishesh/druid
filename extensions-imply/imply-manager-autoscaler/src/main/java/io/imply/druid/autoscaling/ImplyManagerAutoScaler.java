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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.imply.druid.autoscaling.client.ImplyManagerServiceClient;
import org.apache.druid.indexing.overlord.autoscaling.AutoScaler;
import org.apache.druid.indexing.overlord.autoscaling.AutoScalingData;
import org.apache.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningConfig;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This module permits the autoscaling of the workers in Imply Manager
 */
@JsonTypeName("implyManager")
public class ImplyManagerAutoScaler implements AutoScaler<ImplyManagerEnvironmentConfig>
{
  private static final EmittingLogger log = new EmittingLogger(ImplyManagerAutoScaler.class);

  private final ImplyManagerEnvironmentConfig envConfig;
  private final int minNumWorkers;
  private final int maxNumWorkers;
  private final ImplyManagerServiceClient implyManagerServiceClient;
  private final SimpleWorkerProvisioningConfig config;

  @JsonCreator
  public ImplyManagerAutoScaler(
      @JsonProperty("minNumWorkers") int minNumWorkers,
      @JsonProperty("maxNumWorkers") int maxNumWorkers,
      @JsonProperty("envConfig") ImplyManagerEnvironmentConfig envConfig,
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
  public ImplyManagerEnvironmentConfig getEnvConfig()
  {
    return envConfig;
  }

  @Override
  public AutoScalingData provision()
  {
    try {
      List<String> before = getStartingOrRunningInstances();
      log.debug("Existing instances [%s]", String.join(",", before));

      int toSize = Math.min(before.size() + 1, getMaxNumWorkers());
      if (before.size() >= toSize) {
        // nothing to scale
        log.info(
            "Skip scaling up as max number of instances reached. currentSize: %d and maxNumWorkers: %d",
            before.size(), getMaxNumWorkers()
        );
        return new AutoScalingData(new ArrayList<>());
      }
      log.info("Asked to provision instances, will resize to %d", toSize);

      if (config.getWorkerVersion() == null) {
        log.error("Unable to provision new instance as worker version is not configured");
        return new AutoScalingData(new ArrayList<>());
      }
      List<String> instanceCreated = implyManagerServiceClient.provisionInstances(envConfig, config.getWorkerVersion(), 1);
      return new AutoScalingData(instanceCreated);
    }
    catch (Exception e) {
      log.error(e, "Unable to provision any instances with Imply Manager.");
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

  @VisibleForTesting
  List<String> getStartingOrRunningInstances()
  {
    ArrayList<String> ids = new ArrayList<>();
    try {
      List<Instance> response = implyManagerServiceClient.listInstances(envConfig);
      for (Instance instance : response) {
        if (Instance.Status.STARTING.equals(instance.getStatus()) ||
            Instance.Status.RUNNING.equals(instance.getStatus())) {
          ids.add(instance.getId());
        }
      }
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
      List<Instance> response = implyManagerServiceClient.listInstances(envConfig);
      ids.addAll(response.stream().filter(instance -> ips.contains(instance.getIp())).map(instance -> instance.getId()).collect(Collectors.toList()));
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
      List<Instance> response = implyManagerServiceClient.listInstances(envConfig);
      ips.addAll(response.stream().filter(instance -> nodeIds.contains(instance.getId())).map(instance -> instance.getIp()).collect(Collectors.toList()));
    }
    catch (Exception e) {
      log.error(e, "Unable to get instances.");
    }
    return ips;
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
    ImplyManagerAutoScaler that = (ImplyManagerAutoScaler) o;
    return minNumWorkers == that.minNumWorkers &&
           maxNumWorkers == that.maxNumWorkers &&
           Objects.equals(envConfig, that.envConfig) &&
           Objects.equals(implyManagerServiceClient, that.implyManagerServiceClient) &&
           Objects.equals(config, that.config);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(envConfig, minNumWorkers, maxNumWorkers, implyManagerServiceClient, config);
  }
}
