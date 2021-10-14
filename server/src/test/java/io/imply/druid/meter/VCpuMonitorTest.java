/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.meter;

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.utils.RuntimeInfo;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class VCpuMonitorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File procDir;
  private File cgroupDir;
  private CgroupDiscoverer discoverer;

  @Before
  public void setUp() throws IOException
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
    final File cpuDir = new File(
        cgroupDir,
        "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );

    Assert.assertTrue((cpuDir.isDirectory() && cpuDir.exists()) || cpuDir.mkdirs());
    TestUtils.copyOrReplaceResource("/cpu.shares", new File(cpuDir, "cpu.shares"));
    TestUtils.copyOrReplaceResource("/cpu.cfs_quota_us", new File(cpuDir, "cpu.cfs_quota_us"));
    TestUtils.copyOrReplaceResource("/cpu.cfs_period_us", new File(cpuDir, "cpu.cfs_period_us"));

    final File cpusetDir = new File(
        cgroupDir,
        "cpuset/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );
    Assert.assertTrue((cpusetDir.isDirectory() && cpusetDir.exists()) || cpusetDir.mkdirs());

    TestUtils.copyOrReplaceResource("/cpuset.cpus", new File(cpusetDir, "cpuset.cpus"));
    TestUtils.copyOrReplaceResource("/cpuset.effective_cpus.complex", new File(cpusetDir, "cpuset.effective_cpus"));
    TestUtils.copyOrReplaceResource("/cpuset.mems", new File(cpusetDir, "cpuset.mems"));
    TestUtils.copyOrReplaceResource("/cpuset.effective_mems", new File(cpusetDir, "cpuset.effective_mems"));

    File kernelDir = new File(
        procDir,
        "sys/kernel/random"
    );
    Assert.assertTrue(kernelDir.mkdirs());
    TestUtils.copyResource("/cpuinfo", new File(procDir, "cpuinfo"));
    TestUtils.copyResource("/boot_id", new File(kernelDir, "boot_id"));
  }

  @Test
  public void testMonitorWithoutCgroup()
  {
    final VCpuMonitor monitor = new VCpuMonitor(
        QueryStackTests.getProcessingConfig(true, 2),
        new MockRuntimeInfo(4, 0, 0),
        Paths.get("/fake/root"),
        Paths.get("/fake/pid"),
        Duration.ofSeconds(0)
    );

    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assert.assertTrue(monitor.doMonitor(emitter));
    Assert.assertTrue(monitor.doMonitor(emitter));

    final List<Event> actualEvents = emitter.getEvents();
    Assert.assertEquals(2, actualEvents.size());
    final Map<String, Object> event = actualEvents.get(0).toMap();
    Assert.assertEquals("meter/vcpus_available", event.get("metric"));
    Assert.assertEquals(4.0, event.get("value"));
    Assert.assertEquals(new UUID(0, 0).toString(), event.get("bootId"));
    Assert.assertEquals(1, event.get("numThreads"));
    Assert.assertEquals(4L, event.get("hostProcessors"));
    Assert.assertEquals(-1L, event.get("processorShares"));
    Assert.assertEquals(-1.0, event.get("processorQuota"));
    Assert.assertEquals(4, event.get("effectiveCpus"));
  }

  @Test
  public void testMonitorWithCgroup()
  {
    final VCpuMonitor monitor = new VCpuMonitor(
        QueryStackTests.getProcessingConfig(true, 2),
        new MockRuntimeInfo(4, 0, 0),
        procDir.toPath(),
        procDir.toPath(),
        Duration.ofSeconds(0)
    );

    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assert.assertTrue(monitor.doMonitor(emitter));
    Assert.assertTrue(monitor.doMonitor(emitter));

    // Expected values come from cpu*, cpuset* and some proc files
    final List<Event> actualEvents = emitter.getEvents();
    Assert.assertEquals(2, actualEvents.size());
    final Map<String, Object> event = actualEvents.get(0).toMap();
    Assert.assertEquals("meter/vcpus_available", event.get("metric"));
    Assert.assertEquals(3.0, event.get("value"));
    Assert.assertEquals("ad1f0a5c-55ea-4a49-9db8-bbb0f22e2ba6", event.get("bootId"));
    Assert.assertEquals(1, event.get("numThreads"));
    Assert.assertEquals(8L, event.get("hostProcessors"));
    Assert.assertEquals(1024L, event.get("processorShares"));
    Assert.assertEquals(3.0, event.get("processorQuota"));
    Assert.assertEquals(7, event.get("effectiveCpus"));
  }

  @Test
  public void testWithEmitterGate()
  {
    Clock mock = EasyMock.createMock(Clock.class);
    // Time request in constructor
    EasyMock.expect(mock.instant()).andReturn(Instant.parse("2021-10-12T11:15:30Z"));
    // Time requests for doMonitor calls
    EasyMock.expect(mock.instant()).andReturn(Instant.parse("2021-10-12T11:16:30Z"));
    EasyMock.expect(mock.instant()).andReturn(Instant.parse("2021-10-12T11:20:30Z"));
    EasyMock.expect(mock.instant()).andReturn(Instant.parse("2021-10-12T11:24:30Z"));
    EasyMock.replay(mock);
    final VCpuMonitor monitor = new VCpuMonitor(
        QueryStackTests.getProcessingConfig(true, 2),
        new MockRuntimeInfo(4, 0, 0),
        procDir.toPath(),
        procDir.toPath(),
        Duration.ofMinutes(10),
        mock
    );

    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");

    Assert.assertTrue(monitor.doMonitor(emitter));
    Assert.assertEquals(0, emitter.getEvents().size());
    Assert.assertEquals(Instant.parse("2021-10-12T11:20:00Z"), monitor.getNextScheduledTime());
    Assert.assertTrue(monitor.doMonitor(emitter));
    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals(Instant.parse("2021-10-12T11:30:00Z"), monitor.getNextScheduledTime());
    Assert.assertTrue(monitor.doMonitor(emitter));
    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals(Instant.parse("2021-10-12T11:30:00Z"), monitor.getNextScheduledTime());
  }

  @Test
  public void testTimeIncrementComputation()
  {
    assertTimeIncrementComputation("2021-10-12T11:15:30Z", "2021-10-12T11:15:30Z", Duration.ZERO);
    assertTimeIncrementComputation("2021-10-12T11:20:00Z", "2021-10-12T11:15:30Z", Duration.ofMinutes(10));
    assertTimeIncrementComputation("2021-10-12T11:30:00Z", "2021-10-12T11:20:00Z", Duration.ofMinutes(10));
    assertTimeIncrementComputation("2021-10-13T00:00:00Z", "2021-10-12T23:55:30Z", Duration.ofMinutes(10));
  }

  @Test
  public void testComputedVCpus()
  {
    assertComputedVCpus(4.0, -1, 0, 4);
    assertComputedVCpus(2.0, -1, 2, 4);
    assertComputedVCpus(3.0, 3, 0, 4);
    assertComputedVCpus(2.0, 3, 2, 4);
    assertComputedVCpus(4.0, 8, 0, 4);
  }

  private static void assertComputedVCpus(
      double expected,
      double processorQuota,
      long effectiveCpuCount,
      long processorCount
  )
  {
    Assert.assertEquals(
        expected,
        VCpuMonitor.getComputedVCpus(processorQuota, effectiveCpuCount, processorCount),
        0
    );
  }

  private static void assertTimeIncrementComputation(
      String expectedTime,
      String inputTime,
      Duration duration
  )
  {
    Assert.assertEquals(
        Instant.parse(expectedTime),
        VCpuMonitor.computeRoundedIncrement(Instant.parse(inputTime), duration)
    );
  }

  static class MockRuntimeInfo extends RuntimeInfo
  {
    private final int availableProcessors;
    private final long maxHeapSize;
    private final long directSize;

    MockRuntimeInfo(int availableProcessors, long directSize, long maxHeapSize)
    {
      this.availableProcessors = availableProcessors;
      this.directSize = directSize;
      this.maxHeapSize = maxHeapSize;
    }

    @Override
    public int getAvailableProcessors()
    {
      return availableProcessors;
    }

    @Override
    public long getMaxHeapSizeBytes()
    {
      return maxHeapSize;
    }

    @Override
    public long getDirectMemorySizeBytes()
    {
      return directSize;
    }
  }
}
