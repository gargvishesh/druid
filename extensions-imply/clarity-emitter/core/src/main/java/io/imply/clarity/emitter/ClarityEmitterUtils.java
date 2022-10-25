/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class ClarityEmitterUtils
{
  private static final Logger log = new Logger(ClarityEmitterUtils.class);

  private static final int MAX_EVENT_SIZE = 1023 * 1024; // Set max size slightly less than 1M to allow for metadata
  private static final String IMPLY_NODE_TYPE_KEY = "implyNodeType";
  private static final String IMPLY_CLUSTER_KEY = "implyCluster";
  private static final String IMPLY_VERSION_KEY = "implyVersion";
  private static final String IMPLY_DRUID_VERSION_KEY = "implyDruidVersion";
  private static final String IMPLY_VERSION_FILE = "imply.version";
  private static final String UNKNOWN_VERSION = "unknown";

  // metric fields that should be masked if anonymization is enabled
  private static final List<String> SENSITIVE_KEYS = ImmutableList.of(
      "identity",
      "remoteAddress"
  );

  public static String getImplyVersion()
  {
    final InputStream in = DruidMetrics.class.getClassLoader().getResourceAsStream(IMPLY_VERSION_FILE);
    if (in != null) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String firstLine = br.readLine();
        return (firstLine == null ? UNKNOWN_VERSION : firstLine);
      }
      catch (Exception e) {
        log.info(e, "Failed to read Imply version from [%s]", IMPLY_VERSION_FILE);
      }
    } else {
      log.debug("Could not open [%s] to read Imply version", IMPLY_VERSION_FILE);
    }

    return UNKNOWN_VERSION;
  }

  public static int getMaxEventSize()
  {
    return MAX_EVENT_SIZE;
  }

  public static String getDruidVersion()
  {
    final String version = DruidMetrics.class.getPackage().getImplementationVersion();
    return version == null ? UNKNOWN_VERSION : version;
  }

  public static long getMemoryBound()
  {
    // min(100MB, 10% of memory)

    return Math.min(
        100 * 1024 * 1024,
        Runtime.getRuntime().maxMemory() / 10
    );
  }

  @Nullable
  public static byte[] encodeEvent(
      final Event event,
      final ClarityNodeDetails nodeDetails,
      final BaseClarityEmitterConfig config,
      final ObjectMapper jsonMapper
  )
  {
    final boolean shouldDrop = ClarityEmitterUtils.shouldDropEvent(
        event,
        nodeDetails,
        config.getSampledMetrics(),
        config.getSampledNodeTypes(),
        config.getSamplingRate()
    );

    if (shouldDrop) {
      return null;
    }

    final Map<String, Object> eventMap = ClarityEmitterUtils.getModifiedEventMap(
        event,
        nodeDetails,
        config,
        jsonMapper
    );

    final byte[] eventBytes;
    try {
      eventBytes = jsonMapper.writeValueAsBytes(eventMap);
    }
    catch (IOException e) {
      // Should never happen. Throw out the error in a fit of pique.
      throw Throwables.propagate(e);
    }

    if (eventBytes.length > MAX_EVENT_SIZE) {
      log.error(
          "Event too large to emit (%,d > %,d): %s ...",
          eventBytes.length,
          MAX_EVENT_SIZE,
          StringUtils.fromUtf8(ByteBuffer.wrap(eventBytes), 1024)
      );
      return null;
    }

    return eventBytes;
  }

  @VisibleForTesting
  public static Map<String, Object> getModifiedEventMap(
      final Event event,
      final ClarityNodeDetails nodeDetails,
      final BaseClarityEmitterConfig config,
      final ObjectMapper jsonMapper
  )
  {
    Map<String, Object> newMap;

    // RequestLogEvent doesn't implement toMap() so convert it from the Jackson annotations
    try {
      newMap = jsonMapper.convertValue(event, new TypeReference<Map<String, Object>>() {});
    }
    catch (Exception e) {
      newMap = new HashMap<>(event.toMap());
    }

    if (config.getContext() != null) {
      for (Map.Entry<String, Object> entry : config.getContext().entrySet()) {
        newMap.putIfAbsent(entry.getKey(), entry.getValue());
      }
    }

    newMap.put(IMPLY_NODE_TYPE_KEY, nodeDetails.getNodeType());
    newMap.put(IMPLY_CLUSTER_KEY, nodeDetails.getClusterName());
    newMap.put(IMPLY_DRUID_VERSION_KEY, nodeDetails.getDruidVersion());
    newMap.put(IMPLY_VERSION_KEY, nodeDetails.getImplyVersion());

    if (config.isAnonymous()) {
      newMap.remove("host");

      for (String sensitiveKey : SENSITIVE_KEYS) {
        Object sensitiveValue = newMap.get(sensitiveKey);
        if (sensitiveValue != null) {
          String anonymizedValue = DigestUtils.sha256Hex(sensitiveValue + "-" + config.getClusterName());
          newMap.put(sensitiveKey, anonymizedValue);
        }
      }
    }

    return newMap;
  }

  private static boolean shouldDropEvent(
      final Event event,
      final ClarityNodeDetails nodeDetails,
      final Set<String> sampledMetrics,
      final Set<String> sampledNodeTypes,
      final int samplingRate
  )
  {
    if (!(event instanceof ServiceMetricEvent)) {
      return false;
    }

    final ServiceMetricEvent serviceMetricEvent = (ServiceMetricEvent) event;

    if (samplingRate == 100
        || serviceMetricEvent.getMetric() == null
        || !sampledNodeTypes.contains(nodeDetails.getNodeType())
        || !sampledMetrics.contains(serviceMetricEvent.getMetric())) {
      return false;
    }

    final Object id = serviceMetricEvent.getUserDims().get("id");
    if (id == null) {
      return ThreadLocalRandom.current().nextInt(100) > samplingRate;
    } else {
      return Math.abs(id.hashCode() % 100) > samplingRate;
    }
  }
}