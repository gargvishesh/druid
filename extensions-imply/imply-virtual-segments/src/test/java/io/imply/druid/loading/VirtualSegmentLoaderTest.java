/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.loading;

import io.imply.druid.TestData;
import io.imply.druid.VirtualSegmentConfig;
import io.imply.druid.segment.VirtualReferenceCountingSegment;
import io.imply.druid.segment.VirtualSegment;
import io.imply.druid.segment.VirtualSegmentStateManager;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class VirtualSegmentLoaderTest
{
  private SegmentLocalCacheManager physicalManager;
  private VirtualSegmentStateManager segmentHolder;
  private SegmentLoaderConfig loaderConfig;
  private VirtualSegmentConfig config;
  private SegmentizerFactory segmentizerFactory;
  private VirtualSegmentStats virtualSegmentStats;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setup() throws IOException
  {
    physicalManager = Mockito.mock(SegmentLocalCacheManager.class);
    segmentizerFactory = Mockito.mock(SegmentizerFactory.class);
    segmentHolder = Mockito.mock(VirtualSegmentStateManager.class);
    config = new VirtualSegmentConfig(1, 1L);
    loaderConfig = new SegmentLoaderConfig().withLocations(Collections.singletonList(new StorageLocationConfig(
        tempFolder.newFolder(),
        10_000L,
        0.0d
    )));
    virtualSegmentStats = new VirtualSegmentStats();
  }

  @Test
  public void downloadNextSegment() throws SegmentLoadingException, IOException
  {
    File parentDir = tempFolder.newFolder();
    VirtualSegmentLoader cacheManager = newVirtualSegmentLoader();
    VirtualReferenceCountingSegment firstSegment = TestData.buildVirtualSegment(1);
    VirtualReferenceCountingSegment secondSegment = TestData.buildVirtualSegment(2);
    virtualSegmentStats.getDownloadThroughputBytesPerSecond();
    Mockito.when(physicalManager.reserve(ArgumentMatchers.any())).thenReturn(true);
    Mockito
        .when(physicalManager.getSegmentFiles(ArgumentMatchers.any()))
        .thenAnswer(args -> {
          //Adding sleep for metrics
          Thread.sleep(100);
          return new File(parentDir, ((DataSegment) args.getArgument(0)).getId().toString());
        });
    Mockito
        .when(segmentizerFactory.factorize(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.eq(false),
            ArgumentMatchers.eq(null)
        ))
        .thenAnswer(args -> toRealSegment(args.getArgument(0), parentDir));
    Mockito.when(segmentHolder.toDownload()).thenReturn(firstSegment).thenReturn(secondSegment).thenReturn(null);
    cacheManager.downloadNextSegment();
    Assert.assertTrue(virtualSegmentStats.getDownloadThroughputBytesPerSecond() > 0L);
    Mockito.verify(physicalManager, Mockito.times(2)).getSegmentFiles(ArgumentMatchers.any());
    Mockito.verify(segmentHolder).downloaded(firstSegment);
    Mockito.verify(segmentHolder).downloaded(secondSegment);
    Assert.assertEquals(new TestSegment(toDataSegment(firstSegment), parentDir), firstSegment.getRealSegment());
    Assert.assertEquals(new TestSegment(toDataSegment(secondSegment), parentDir), secondSegment.getRealSegment());
  }

  @Test
  public void testDownloadAndEvict() throws SegmentLoadingException, IOException
  {
    File parentDir = tempFolder.newFolder();
    VirtualSegmentLoader cacheManager = newVirtualSegmentLoader();
    VirtualReferenceCountingSegment firstSegment = TestData.buildVirtualSegment(1);
    DataSegment dataSegment = toDataSegment(firstSegment);
    VirtualReferenceCountingSegment secondSegment = TestData.buildVirtualSegment(2);
    Mockito.when(segmentHolder.toDownload()).thenReturn(firstSegment).thenReturn(null);
    Mockito.when(segmentHolder.toEvict()).thenReturn(secondSegment);
    Mockito.when(physicalManager.reserve(dataSegment)).thenReturn(false, false, false, true);
    Mockito
        .when(physicalManager.getSegmentFiles(ArgumentMatchers.any()))
        .thenAnswer(args -> new File(parentDir, ((DataSegment) args.getArgument(0)).getId().toString()));
    Mockito
        .when(segmentizerFactory.factorize(
            ArgumentMatchers.any(),
            ArgumentMatchers.any(),
            ArgumentMatchers.eq(false),
            ArgumentMatchers.eq(null)
        ))
        .thenAnswer(args -> toRealSegment(args.getArgument(0), parentDir));
    cacheManager.downloadNextSegment();
    Mockito.verify(physicalManager, Mockito.times(3)).cleanup(toDataSegment(secondSegment));
    Mockito.verify(segmentHolder, Mockito.times(3)).evict(secondSegment);
    Assert.assertEquals(new TestSegment(dataSegment, parentDir), firstSegment.getRealSegment());
  }

  @Test
  public void testDownloadNotEnoughSpace() throws SegmentLoadingException
  {
    VirtualSegmentLoader cacheManager = newVirtualSegmentLoader();
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment(1);
    DataSegment dataSegment = toDataSegment(segment);
    Mockito.when(segmentHolder.toDownload()).thenReturn(segment).thenReturn(null);
    Mockito.when(segmentHolder.toEvict()).thenReturn(null);
    Mockito.when(physicalManager.reserve(dataSegment)).thenReturn(false);
    try {
      cacheManager.start();
      Mockito.verify(segmentHolder, Mockito.timeout(5000L)).requeue(segment);
      Mockito.verify(physicalManager, Mockito.never()).getSegmentFiles(ArgumentMatchers.any());
      Assert.assertNull(segment.getRealSegment());
    }
    finally {
      cacheManager.stop();
    }
  }

  @Test
  public void testScheduleDownload()
  {
    VirtualSegmentLoader cacheManager = newVirtualSegmentLoader();
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment(1);
    try {
      cacheManager.start();
      Closer closer = Closer.create();
      cacheManager.scheduleDownload(segment, closer);
      Mockito.verify(segmentHolder).queue(segment, closer);
    }
    finally {
      cacheManager.stop();
    }
  }

  @Test(expected = ISE.class)
  public void testExceptionScheduleDownloadAfterStop()
  {
    VirtualSegmentLoader cacheManager = newVirtualSegmentLoader();
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment(1);
    cacheManager.start();
    cacheManager.stop();
    cacheManager.scheduleDownload(segment, Closer.create());
  }

  @Test(expected = ISE.class)
  public void testExceptionScheduleDownloadBeforeStart()
  {
    VirtualSegmentLoader cacheManager = newVirtualSegmentLoader();
    VirtualReferenceCountingSegment segment = TestData.buildVirtualSegment(1);
    cacheManager.scheduleDownload(segment, Closer.create());
  }

  @Test
  public void testGetSegment() throws SegmentLoadingException
  {
    VirtualSegmentLoader cacheManager = newVirtualSegmentLoader();
    DataSegment dataSegment = TestData.buildDataSegment(1);
    Mockito.when(segmentHolder.registerIfAbsent(ArgumentMatchers.any()))
           .thenAnswer(invocation -> invocation.getArgument(0));
    ReferenceCountingSegment virtualSegment = cacheManager.getSegment(dataSegment, true, () -> {
    });
    Assert.assertNotNull(virtualSegment);
    Assert.assertEquals(VirtualReferenceCountingSegment.class, virtualSegment.getClass());
    Assert.assertEquals(dataSegment.getId(), virtualSegment.getId());

    Mockito.when(segmentHolder.registerIfAbsent(ArgumentMatchers.any()))
           .thenReturn((VirtualReferenceCountingSegment) virtualSegment);
    ReferenceCountingSegment otherVirtualSegment = cacheManager.getSegment(dataSegment, true, () -> {
    });
    Assert.assertSame(virtualSegment, otherVirtualSegment);
  }

  private DataSegment toDataSegment(VirtualReferenceCountingSegment segment)
  {
    return ((VirtualSegment) Objects.requireNonNull(segment.getBaseSegment())).asDataSegment();
  }

  private Segment toRealSegment(DataSegment segment, File parentDir)
  {
    return new TestSegment(segment, parentDir);
  }

  private VirtualSegmentLoader newVirtualSegmentLoader()
  {
    return new VirtualSegmentLoader(
        physicalManager,
        loaderConfig,
        segmentHolder,
        config,
        null,
        segmentizerFactory,
        virtualSegmentStats
    );
  }

  private static class TestSegment implements Segment
  {

    private final DataSegment dataSegment;
    private File segmentDir;

    public TestSegment(DataSegment dataSegment, File parentDir)
    {
      this.dataSegment = dataSegment;
      this.segmentDir = new File(parentDir, dataSegment.getId().toString());
    }

    @Override
    public SegmentId getId()
    {
      return dataSegment.getId();
    }

    @Override
    public Interval getDataInterval()
    {
      return dataSegment.getInterval();
    }

    @Nullable
    @Override
    public QueryableIndex asQueryableIndex()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public StorageAdapter asStorageAdapter()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {

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
      TestSegment that = (TestSegment) o;
      return dataSegment.equals(that.dataSegment) && segmentDir.equals(that.segmentDir);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(dataSegment, segmentDir);
    }
  }
}
