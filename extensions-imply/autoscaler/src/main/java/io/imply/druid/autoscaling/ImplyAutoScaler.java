/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling;

import com.sun.istack.internal.Nullable;
import io.imply.druid.autoscaling.server.ImplyManagerServiceClient;

import java.io.IOException;
import java.net.NetworkInterface;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

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
  private final int numInstances;
  private final ImplyManagerServiceClient implyManagerServiceClient;

  private static final long POLL_INTERVAL_MS = 5 * 1000;  // 5 sec
  private static final int RUNNING_INSTANCES_MAX_RETRIES = 10;
  private static final int OPERATION_END_MAX_RETRIES = 10;

  @JsonCreator
  public ImplyAutoScaler(
      @JsonProperty("minNumWorkers") int minNumWorkers,
      @JsonProperty("maxNumWorkers") int maxNumWorkers,
      @JsonProperty("numInstances") int numInstances,
      @JsonProperty("envConfig") ImplyEnvironmentConfig envConfig
      @JacksonInject ImplyManagerServiceClient implyManagerServiceClient;
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
    Preconditions.checkArgument(numInstances > 0,
                                "numInstances must be greater than 0");
    this.numInstances = numInstances;
    this.envConfig = envConfig;
    this.implyManagerServiceClient = implyManagerServiceClient;
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


  // Used to wait for an operation to finish
  @Nullable
  private Operation.Error waitForOperationEnd(
      Compute compute,
      Operation operation) throws Exception
  {
    String status = operation.getStatus();
    String opId = operation.getName();
    for (int i = 0; i < OPERATION_END_MAX_RETRIES; i++) {
      if (operation == null || "DONE".equals(status)) {
        return operation == null ? null : operation.getError();
      }
      log.info("Waiting for operation %s to end", opId);
      Thread.sleep(POLL_INTERVAL_MS);
      Compute.ZoneOperations.Get get = compute.zoneOperations().get(
          envConfig.getProjectId(),
          envConfig.getZoneName(),
          opId
      );
      operation = get.execute();
      if (operation != null) {
        status = operation.getStatus();
      }
    }
    throw new InterruptedException(
        StringUtils.format("Timed out waiting for operation %s to complete", opId)
    );
  }

  /**
   * When called resizes envConfig.getManagedInstanceGroupName() increasing it by creating
   * envConfig.getNumInstances() new workers (unless the maximum is reached). Return the
   * IDs of the workers created
   */
  @Override
  public AutoScalingData provision()
  {
    final String project = envConfig.getProjectId();
    final String zone = envConfig.getZoneName();
    final int numInstances = envConfig.getNumInstances();
    final String managedInstanceGroupName = envConfig.getManagedInstanceGroupName();

    try {
      List<String> before = getRunningInstances();
      log.debug("Existing instances [%s]", String.join(",", before));

      int toSize = Math.min(before.size() + numInstances, getMaxNumWorkers());
      if (before.size() >= toSize) {
        // nothing to scale
        return new AutoScalingData(new ArrayList<>());
      }
      log.info("Asked to provision instances, will resize to %d", toSize);

      Compute computeService = createComputeService();
      Compute.InstanceGroupManagers.Resize request =
          computeService.instanceGroupManagers().resize(project, zone,
                                                        managedInstanceGroupName, toSize);

      Operation response = request.execute();
      Operation.Error err = waitForOperationEnd(computeService, response);
      if (err == null || err.isEmpty()) {
        List<String> after = null;
        // as the waitForOperationEnd only waits for the operation to be scheduled
        // this loop waits until the requested machines actually go up (or up to a
        // certain amount of retries in checking)
        for (int i = 0; i < RUNNING_INSTANCES_MAX_RETRIES; i++) {
          after = getRunningInstances();
          if (after.size() == toSize) {
            break;
          }
          log.info("Machines not up yet, waiting");
          Thread.sleep(POLL_INTERVAL_MS);
        }
        after.removeAll(before); // these should be the new ones
        log.info("Added instances [%s]", String.join(",", after));
        return new AutoScalingData(after);
      } else {
        log.error("Unable to provision instances: %s", err.toPrettyString());
      }
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

    List<String> nodeIds = ipToIdLookup(ips); // if they are not IPs, they will be unchanged
    try {
      return terminateWithIds(nodeIds != null ? nodeIds : new ArrayList<>());
    }
    catch (Exception e) {
      log.error(e, "Unable to terminate any instances.");
    }

    return new AutoScalingData(new ArrayList<>());
  }

  private List<String> namesToInstances(List<String> names)
  {
    List<String> instances = new ArrayList<>();
    for (String name : names) {
      instances.add(
          // convert the name into a URL's path to be used in calls to the API
          StringUtils.format("zones/%s/instances/%s", envConfig.getZoneName(), name)
      );
    }
    return instances;
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

    try {
      final String project = envConfig.getProjectId();
      final String zone = envConfig.getZoneName();
      final String managedInstanceGroupName = envConfig.getManagedInstanceGroupName();

      List<String> before = getRunningInstances();

      InstanceGroupManagersDeleteInstancesRequest requestBody =
          new InstanceGroupManagersDeleteInstancesRequest();
      requestBody.setInstances(namesToInstances(ids));

      Compute computeService = createComputeService();
      Compute.InstanceGroupManagers.DeleteInstances request =
          computeService
              .instanceGroupManagers()
              .deleteInstances(project, zone, managedInstanceGroupName, requestBody);

      Operation response = request.execute();
      Operation.Error err = waitForOperationEnd(computeService, response);
      if (err == null || err.isEmpty()) {
        List<String> after = null;
        // as the waitForOperationEnd only waits for the operation to be scheduled
        // this loop waits until the requested machines actually go down (or up to a
        // certain amount of retries in checking)
        for (int i = 0; i < RUNNING_INSTANCES_MAX_RETRIES; i++) {
          after = getRunningInstances();
          if (after.size() == (before.size() - ids.size())) {
            break;
          }
          log.info("Machines not down yet, waiting");
          Thread.sleep(POLL_INTERVAL_MS);
        }
        before.removeAll(after); // keep only the ones no more present
        return new AutoScalingData(before);
      } else {
        log.error("Unable to terminate instances: %s", err.toPrettyString());
      }
    }
    catch (Exception e) {
      log.error(e, "Unable to terminate any instances.");
    }

    return new AutoScalingData(new ArrayList<>());
  }

  // Returns the list of the IDs of the machines running in the MIG
  private List<String> getRunningInstances()
  {
    final long maxResults = 500L; // 500 is sadly the max, see below

    ArrayList<String> ids = new ArrayList<>();
    try {
      final String project = envConfig.getProjectId();
      final String zone = envConfig.getZoneName();
      final String managedInstanceGroupName = envConfig.getManagedInstanceGroupName();

      Compute computeService = createComputeService();
      Compute.InstanceGroupManagers.ListManagedInstances request =
          computeService
              .instanceGroupManagers()
              .listManagedInstances(project, zone, managedInstanceGroupName);
      // Notice that while the doc says otherwise, there is not nextPageToken to page
      // through results and so everything needs to be in the same page
      request.setMaxResults(maxResults);
      InstanceGroupManagersListManagedInstancesResponse response = request.execute();
      for (ManagedInstance mi : response.getManagedInstances()) {
        ids.add(GceUtils.extractNameFromInstance(mi.getInstance()));
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

    // If the first one is not an IP, just assume all the other ones are not as well and just
    // return them as they are. This check is here because Druid does not check if IPs are
    // actually IPs and can send IDs to this function instead
    if (!InetAddresses.isInetAddress(ips.get(0))) {
      log.debug("Not IPs, doing nothing");
      return ips;
    }

    final String project = envConfig.getProjectId();
    final String zone = envConfig.getZoneName();
    try {
      Compute computeService = createComputeService();
      Compute.Instances.List request = computeService.instances().list(project, zone);
      // Cannot filter by IP atm, see below
      // request.setFilter(GceUtils.buildFilter(ips, "networkInterfaces[0].networkIP"));

      List<String> instanceIds = new ArrayList<>();
      InstanceList response;
      do {
        response = request.execute();
        if (response.getItems() == null) {
          continue;
        }
        for (Instance instance : response.getItems()) {
          // This stupid look up is needed because atm it is not possible to filter
          // by IP, see https://issuetracker.google.com/issues/73455339
          for (NetworkInterface ni : instance.getNetworkInterfaces()) {
            if (ips.contains(ni.getNetworkIP())) {
              instanceIds.add(instance.getName());
            }
          }
        }
        request.setPageToken(response.getNextPageToken());
      } while (response.getNextPageToken() != null);

      log.debug("Converted to [%s]", String.join(",", instanceIds));
      return instanceIds;
    }
    catch (Exception e) {
      log.error(e, "Unable to convert IPs to IDs.");
    }

    return new ArrayList<>();
  }

  /**
   * Converts the IDs to IPs - this is actually never called from the outside but it is called once
   * from inside the class if terminate is used instead of terminateWithIds
   */
  @Override
  public List<String> idToIpLookup(List<String> nodeIds)
  {
    log.info("Asked IDs -> IPs for: [%s]", String.join(",", nodeIds));

    if (nodeIds.isEmpty()) {
      return new ArrayList<>();
    }

    final String project = envConfig.getProjectId();
    final String zone = envConfig.getZoneName();

    try {
      Compute computeService = createComputeService();
      Compute.Instances.List request = computeService.instances().list(project, zone);
      request.setFilter(GceUtils.buildFilter(nodeIds, "name"));

      List<String> instanceIps = new ArrayList<>();
      InstanceList response;
      do {
        response = request.execute();
        if (response.getItems() == null) {
          continue;
        }
        for (Instance instance : response.getItems()) {
          // Assuming that every server has at least one network interface...
          String ip = instance.getNetworkInterfaces().get(0).getNetworkIP();
          // ...even though some IPs are reported as null on the spot but later they are ok,
          // so we skip the ones that are null. fear not, they are picked up later this just
          // prevents to have a machine called 'null' around which makes the caller wait for
          // it for maxScalingDuration time before doing anything else
          if (ip != null && !"null".equals(ip)) {
            instanceIps.add(ip);
          } else {
            // log and skip it
            log.warn("Call returned null IP for %s, skipping", instance.getName());
          }
        }
        request.setPageToken(response.getNextPageToken());
      } while (response.getNextPageToken() != null);

      return instanceIps;
    }
    catch (Exception e) {
      log.error(e, "Unable to convert IDs to IPs.");
    }

    return new ArrayList<>();
  }


}
