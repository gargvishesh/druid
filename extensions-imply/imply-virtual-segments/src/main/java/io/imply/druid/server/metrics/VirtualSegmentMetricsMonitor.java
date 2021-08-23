/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.server.metrics;

import io.imply.druid.loading.VirtualSegmentLoader;
import io.imply.druid.loading.VirtualSegmentStats;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import javax.inject.Inject;

/**
 * Monitor for the custom metrics which is collected as part of the virtual segment extension. Metrics are sent to the
 * {@link ServiceEmitter} on periodic invocation of
 * {@link org.apache.druid.java.util.metrics.Monitor#monitor(ServiceEmitter)}.
 */

public class VirtualSegmentMetricsMonitor extends AbstractMonitor
{
  private final VirtualSegmentStats virtualSegmentStats;
  private final VirtualSegmentLoader virtualSegmentLoader;
  private final DruidServerConfig serverConfig;

  @Inject
  public VirtualSegmentMetricsMonitor(
      VirtualSegmentStats virtualSegmentStats,
      DruidServerConfig serverConfig,
      VirtualSegmentLoader virtualSegmentLoader
  )
  {
    this.virtualSegmentStats = virtualSegmentStats;
    this.virtualSegmentLoader = virtualSegmentLoader;
    this.serverConfig = serverConfig;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder =
        new ServiceMetricEvent.Builder().setDimension("tier", serverConfig.getTier());

    emitter.emit(builder.build(
        "virtual/segment/download/throughputBytesPerSecond",
        virtualSegmentStats.getDownloadThroughputBytesPerSecond()
    ));

    emitter.emit(builder.build(
        "virtual/segment/download/waitMS",
        virtualSegmentStats.getAvgDownloadWaitingTimeInMS()
    ));

    emitter.emit(builder.build(
        "virtual/segment/evicted",
        virtualSegmentStats.getNumSegmentsEvicted()
    ));

    emitter.emit(builder.build(
        "virtual/segment/removed",
        virtualSegmentStats.getNumSegmentsRemoved()
    ));

    emitter.emit(builder.build(
        "virtual/segment/queued",
        virtualSegmentStats.getNumSegmentsQueued()
    ));

    emitter.emit(builder.build(
        "virtual/segment/dowloaded",
        virtualSegmentStats.getNumSegmentsDownloaded()
    ));

    virtualSegmentStats.resetMetrics();

    virtualSegmentLoader.getDownloadQueueGuages().forEach(
        (key, value) ->
            emitter.emit(builder.build(key, value)));

    return true;
  }
}
