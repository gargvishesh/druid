/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.meter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.java.util.metrics.CgroupCpuMonitor;
import org.apache.druid.java.util.metrics.ProcFsReader;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.Cpu;
import org.apache.druid.java.util.metrics.cgroups.CpuSet;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupDiscoverer;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.utils.RuntimeInfo;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A monitor that tracks the effective vCPUs available to the process. The metrics are gathered using the
 * procfs and sysfs calls but in the absence of that data, it just falls back to the Java runtime.
 * <p>
 * This monitor has a built-in mechanism that throlles emission to once per 10-minute period. This simplifies
 * calculations on the Druid query side where this data will ultimately reside.
 */
public class VCpuMonitor extends AbstractMonitor
{
  private static final String METRIC = "meter/vcpus_available";
  private static final Path PROC_SELF = Paths.get("/proc/self");
  private static final Duration DEFAULT_EMISSION_PERIOD = Duration.ofMinutes(10);
  private static final Logger LOGGER = new Logger(VCpuMonitor.class);

  private final UUID bootId;
  private final int numThreads;
  private final long hostProcessorCount;
  private final long processorShares;
  private final double processorQuota;
  private final int effectiveCpuCount;
  private final double computedVCpus;
  private final Duration emissionPeriod;
  private final Clock clock;

  private Instant nextScheduledTime;


  @Inject
  public VCpuMonitor(
      DruidProcessingConfig druidProcessingConfig,
      RuntimeInfo runtimeInfo
  )
  {
    this(
        druidProcessingConfig,
        runtimeInfo,
        ProcFsReader.DEFAULT_PROC_FS_ROOT,
        PROC_SELF,
        DEFAULT_EMISSION_PERIOD
    );
  }

  @VisibleForTesting
  VCpuMonitor(
      DruidProcessingConfig druidProcessingConfig,
      RuntimeInfo runtimeInfo,
      Path procFsRoot,
      Path procPidDir,
      Duration emissionPeriod
  )
  {
    this(
        druidProcessingConfig,
        runtimeInfo,
        procFsRoot,
        procPidDir,
        emissionPeriod,
        Clock.systemUTC()
    );
  }

  @VisibleForTesting
  VCpuMonitor(
      DruidProcessingConfig druidProcessingConfig,
      RuntimeInfo runtimeInfo,
      Path procFsRoot,
      Path procPidDir,
      Duration emissionPeriod,
      Clock clock
  )
  {
    this.numThreads = druidProcessingConfig.getNumThreads();
    this.emissionPeriod = emissionPeriod;
    this.clock = clock;
    this.nextScheduledTime = computeRoundedIncrement(Instant.now(clock), emissionPeriod);

    if (procFsRoot.toFile().isDirectory()) {
      LOGGER.info("/proc directory exists. Using cgroup data.");
      CgroupDiscoverer cgroupDiscoverer = new ProcCgroupDiscoverer(procPidDir);
      Cpu.CpuAllocationMetric cpuSnapshot = new Cpu(cgroupDiscoverer).snapshot();
      CpuSet.CpuSetMetric cpuSetSnapshot = new CpuSet(cgroupDiscoverer).snapshot();
      ProcFsReader procFsReader = new ProcFsReader(procFsRoot);

      this.bootId = procFsReader.getBootId();
      this.hostProcessorCount = procFsReader.getProcessorCount();
      this.processorShares = cpuSnapshot.getShares();
      this.processorQuota = CgroupCpuMonitor.computeProcessorQuota(cpuSnapshot.getQuotaUs(), cpuSnapshot.getPeriodUs());
      this.effectiveCpuCount = cpuSetSnapshot.getEffectiveCpuSetCpus().length;
      this.computedVCpus = getComputedVCpus(this.processorQuota, this.effectiveCpuCount, hostProcessorCount);
    } else {
      LOGGER.info("/proc directory does not exist. Falling back to RuntimeInfo.");
      this.bootId = new UUID(0, 0);
      this.hostProcessorCount = runtimeInfo.getAvailableProcessors();
      this.processorShares = -1L;
      this.processorQuota = -1L;
      this.effectiveCpuCount = runtimeInfo.getAvailableProcessors();
      this.computedVCpus = runtimeInfo.getAvailableProcessors();
    }
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    if (nextScheduledTime.toEpochMilli() > Instant.now(clock).toEpochMilli()) {
      return true;
    }

    nextScheduledTime = computeRoundedIncrement(nextScheduledTime, emissionPeriod);

    final ServiceEventBuilder<ServiceMetricEvent> eventBuilder = new ServiceMetricEvent.Builder()
        .setDimension("bootId", bootId.toString())
        .setDimension("numThreads", numThreads)
        .setDimension("hostProcessors", hostProcessorCount)
        .setDimension("processorShares", processorShares)
        .setDimension("processorQuota", processorQuota)
        .setDimension("effectiveCpus", effectiveCpuCount)
        .build(METRIC, computedVCpus);

    emitter.emit(eventBuilder);
    return true;
  }

  public Instant getNextScheduledTime()
  {
    return nextScheduledTime;
  }

  /**
   * Computes the maximum number of processors actually available to this process.
   *
   * @param processorQuota    the computed processor quota
   * @param effectiveCpuCount the effective cpu
   * @param processorCount    the total number of processors available on the host
   * @return the true number of processors available to this process
   */
  @VisibleForTesting
  static double getComputedVCpus(
      double processorQuota,
      long effectiveCpuCount,
      long processorCount
  )
  {
    // If we don't have the CPU list, just use the total processor count
    long trueProcessorCount =
        effectiveCpuCount == 0
        ? processorCount
        : effectiveCpuCount;

    // Any negative quota value means all effective CPUs are available for use. Otherwise, cap it to the
    // effective CPU count in case the quota is set arbitrarily high.
    return processorQuota < 0
           ? trueProcessorCount
           : Math.min(processorQuota, trueProcessorCount);
  }


  /**
   * Generate the next period for a given time, rounded to the nearest start of the duration.
   * For example, if time is "10:22am" and period is "10 minutes", the function will output "10:30am".
   *
   * @param time   the time to be incremented.
   * @param period the period to increment by.
   * @return the incremented time.
   */
  @VisibleForTesting
  static Instant computeRoundedIncrement(Instant time, Duration period)
  {
    Preconditions.checkNotNull(time);
    if (Objects.equals(period, Duration.ZERO)) {
      return time;
    }

    return Instant.ofEpochMilli(
        (time.toEpochMilli() / period.toMillis() + 1) * period.toMillis());
  }
}
