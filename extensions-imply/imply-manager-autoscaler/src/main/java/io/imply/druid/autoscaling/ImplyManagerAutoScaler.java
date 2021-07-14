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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    Preconditions.checkArgument(minNumWorkers >= 0,
                                "minNumWorkers must be greater than or equal to 0");
    this.minNumWorkers = minNumWorkers;
    Preconditions.checkArgument(maxNumWorkers > 0,
                                "maxNumWorkers must be greater than 0");
    Preconditions.checkArgument(maxNumWorkers >= minNumWorkers,
                                "maxNumWorkers must be greater than or equal to minNumWorkers");
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
      List<String> before = getNonTerminatingInstances();
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
      log.info("Asked to provision instances, from %d resize to %d", before.size(), toSize);

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

    try {
      List<String> nodeIds = ipToIdLookup(ips);
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
        implyManagerServiceClient.terminateInstance(envConfig, id);
      }
      catch (Exception e) {
        log.error(e, "Unable to terminate instances: [%s]", id);
      }
    }
    return new AutoScalingData(ids);
  }

  @VisibleForTesting
  List<String> getNonTerminatingInstances()
  {
    ArrayList<String> ids = new ArrayList<>();
    try {
      List<Instance> response = implyManagerServiceClient.listInstances(envConfig);
      for (Instance instance : response) {
        if (!Instance.Status.TERMINATING.equals(instance.getStatus())) {
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
      Map<String, String> ipToIdMap = new HashMap<>();
      for (Instance instance : response) {
        if (instance.getIp() != null) {
          ipToIdMap.put(instance.getIp(), instance.getId());
        }
      }
      for (String ip : ips) {
        if (ipToIdMap.containsKey(ip)) {
          ids.add(ipToIdMap.get(ip));
        } else {
          log.warn("Cannot find instance for ip: %s", ip);
        }
      }
      return ids;
    }
    catch (Exception e) {
      log.error(e, "Unable to get instances.");
      throw new RuntimeException(e);
    }
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
      Map<String, String> idToIp = new HashMap<>();
      for (Instance instance : response) {
        if (instance.getIp() != null) {
          idToIp.put(instance.getId(), instance.getIp());
        }
      }
      for (String id : nodeIds) {
        if (idToIp.containsKey(id)) {
          ips.add(idToIp.get(id));
        } else {
          log.warn("Cannot find instance for id: %s", id);
        }
      }
      return ips;
    }
    catch (Exception e) {
      log.error(e, "Unable to get instances.");
      throw new RuntimeException(e);
    }
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
