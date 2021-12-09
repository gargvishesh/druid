/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.imply.druid.license.TestingImplyLicenseManager;
import io.imply.druid.loading.VirtualSegmentLoader;
import io.imply.druid.segment.VirtualReferenceCountingSegment;
import io.imply.druid.segment.VirtualSegment;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.RandomStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationSelectorStrategy;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.DataSegmentChangeCallback;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordination.TestStorageLocation;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class includes tests that cover the storage location layer as well.
 */
public class ITVirtualSegmentLoaderTest
{
  private static final long MAX_SIZE = 1000L;
  private static final long SEGMENT_SIZE = 100L;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private TestStorageLocation storageLoc;
  private ObjectMapper objectMapper;
  private SegmentLoaderConfig config;
  private List<StorageLocation> locations;
  private Interval interval;

  @Before
  public void setup() throws IOException
  {
    storageLoc = new TestStorageLocation(temporaryFolder);
    objectMapper = TestHelper.makeJsonMapper();
    objectMapper.registerSubtypes(TestLoadSpec.class);
    objectMapper.registerSubtypes(TestSegmentizerFactory.class);
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    config = new SegmentLoaderConfig()
        .withLocations(Collections.singletonList(storageLoc.toStorageLocationConfig(MAX_SIZE, null)))
        .withInfoDir(storageLoc.getInfoDir());
    locations = config.toStorageLocations();
    interval = Intervals.utc(System.currentTimeMillis() - 60 * 1000, System.currentTimeMillis());
  }

  @Test
  public void testAddSegment()
      throws InterruptedException, ExecutionException, TimeoutException, IOException
  {
    Injector injector = setupInjector();
    SegmentLoadDropHandler handler = injector.getInstance(SegmentLoadDropHandler.class);
    handler.addSegment(makeSegment("test", "segment-1"), DataSegmentChangeCallback.NOOP);
    handler.addSegment(makeSegment("test", "segment-2"), DataSegmentChangeCallback.NOOP);
    SegmentManager segmentManager = injector.getInstance(SegmentManager.class);
    VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = segmentManager
        .getTimeline(DataSourceAnalysis.forDataSource(new TableDataSource("test")))
        .orElseThrow(() -> new IllegalStateException("Null timeline"));
    Assert.assertEquals(2, timeline.getNumObjects());
    VirtualReferenceCountingSegment segment = (VirtualReferenceCountingSegment) timeline
        .findChunk(interval, "segment-1", 0)
        .getObject();

    Assert.assertTrue(segment.getBaseSegment() instanceof VirtualSegment);

    VirtualSegment virtualSegment = (VirtualSegment) segment.getBaseSegment();
    Assert.assertNull(virtualSegment.getRealSegment());

    VirtualSegmentLoader cacheManager = injector.getInstance(VirtualSegmentLoader.class);
    try (Closer segmentResource = Closer.create()) {
      cacheManager.start();
      ListenableFuture<Void> future = cacheManager.scheduleDownload(segment, segmentResource);
      Assert.assertTrue(segment.hasActiveQueries());

      future.get(10, TimeUnit.SECONDS);
      Assert.assertNotNull(virtualSegment.getRealSegment());
      Assert.assertNotNull(virtualSegment.asQueryableIndex());
      Assert.assertNotNull(virtualSegment.asStorageAdapter());
    }
    finally {
      cacheManager.stop();
    }
  }

  // Disable till https://implydata.atlassian.net/browse/IMPLY-13843 is resolved.
  @Ignore
  @Test(expected = TimeoutException.class)
  public void testScheduleDownloadWithNoQuery()
      throws InterruptedException, ExecutionException, TimeoutException, IOException
  {
    Injector injector = setupInjector();
    SegmentLoadDropHandler handler = injector.getInstance(SegmentLoadDropHandler.class);
    handler.addSegment(makeSegment("test", "segment-1"), DataSegmentChangeCallback.NOOP);
    handler.addSegment(makeSegment("test", "segment-2"), DataSegmentChangeCallback.NOOP);
    SegmentManager segmentManager = injector.getInstance(SegmentManager.class);
    VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = segmentManager
        .getTimeline(DataSourceAnalysis.forDataSource(new TableDataSource("test")))
        .orElseThrow(() -> new IllegalStateException("Null timeline"));
    Assert.assertEquals(2, timeline.getNumObjects());
    VirtualReferenceCountingSegment segment = (VirtualReferenceCountingSegment) timeline
        .findChunk(interval, "segment-1", 0)
        .getObject();
    Assert.assertTrue(segment.getBaseSegment() instanceof VirtualSegment);

    VirtualSegment virtualSegment = (VirtualSegment) segment.getBaseSegment();
    Assert.assertNull(virtualSegment.getRealSegment());

    VirtualSegmentLoader cacheManager = injector.getInstance(VirtualSegmentLoader.class);
    try {
      // Try to download the segment
      cacheManager.start();
      Closer segmentResource = Closer.create();
      ListenableFuture<Void> future = cacheManager.scheduleDownload(segment, segmentResource);

      // Verify that closing the resource removes active queries from the segment
      Assert.assertTrue(segment.hasActiveQueries());
      segmentResource.close();
      Assert.assertFalse(segment.hasActiveQueries());

      future.get(10, TimeUnit.SECONDS);
    }
    finally {
      cacheManager.stop();

      // Verify that download did not succeed
      Assert.assertNull(virtualSegment.getRealSegment());
    }
  }

  private Injector setupInjector()
  {
    Properties properties = new Properties();
    properties.setProperty(VirtualSegmentModule.ENABLED_PROPERTY, "true");
    VirtualSegmentModule virtualSegmentModule = new VirtualSegmentModule(properties);
    virtualSegmentModule.setImplyLicenseManager(new TestingImplyLicenseManager(null));
    return Guice.createInjector(Modules.override(virtualSegmentModule, new LifecycleModule())
                                       .with(new StorageModule()));
  }

  private DataSegment makeSegment(String dataSource, String name)
  {
    return new DataSegment(
        dataSource,
        interval,
        name,
        ImmutableMap.of("type", "test", "name", name, "size", SEGMENT_SIZE),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        SEGMENT_SIZE
    );
  }

  @JsonTypeName("test")
  public static class TestLoadSpec implements LoadSpec
  {

    private final int size;
    private final String name;

    @JsonCreator
    public TestLoadSpec(
        @JsonProperty("size") int size,
        @JsonProperty("name") String name
    )
    {
      this.size = size;
      this.name = name;
    }

    @Override
    public LoadSpecResult loadSegment(File destDir) throws SegmentLoadingException
    {
      File segmentFile = new File(destDir, "segment");
      File factoryJson = new File(destDir, "factory.json");
      try {
        FileUtils.mkdirp(destDir);
        segmentFile.createNewFile();
        factoryJson.createNewFile();
      }
      catch (IOException e) {
        throw new SegmentLoadingException(
            e,
            "Failed to create files under dir '%s'",
            destDir.getAbsolutePath()
        );
      }

      try {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        Files.write(bytes, segmentFile);
        Files.write("{\"type\":\"testSegmentFactory\"}".getBytes(StandardCharsets.UTF_8), factoryJson);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(
            e,
            "Failed to write data in directory %s",
            destDir.getAbsolutePath()
        );
      }
      return new LoadSpecResult(size);
    }
  }

  @JsonTypeName("testSegmentFactory")
  public static class TestSegmentizerFactory implements SegmentizerFactory
  {

    @Override
    public Segment factorize(
        DataSegment dataSegment,
        File parentDir,
        boolean lazy,
        SegmentLazyLoadFailCallback loadFailed
    )
    {
      Segment segment = Mockito.mock(Segment.class);
      Mockito.when(segment.asQueryableIndex()).thenReturn(Mockito.mock(QueryableIndex.class));
      Mockito.when(segment.asStorageAdapter()).thenReturn(Mockito.mock(StorageAdapter.class));
      return segment;
    }
  }

  private class StorageModule extends AbstractModule
  {

    @Override
    protected void configure()
    {
      bind(DataSegmentAnnouncer.class).toInstance(Mockito.mock(DataSegmentAnnouncer.class));
      bind(DataSegmentServerAnnouncer.class).toInstance(Mockito.mock(DataSegmentServerAnnouncer.class));
      bind(SegmentLoaderConfig.class).toInstance(config);
      bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(objectMapper);
      bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.HISTORICAL));
      bind(QueryRunnerFactoryConglomerate.class).toInstance(Mockito.mock(QueryRunnerFactoryConglomerate.class));
      bind(QuerySegmentWalker.class).toInstance(Mockito.mock(QuerySegmentWalker.class));
      bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
      bind(StorageLocationSelectorStrategy.class).toInstance(new RandomStorageLocationSelectorStrategy(locations));
      bind(ColumnConfig.class).toInstance(() -> 1024);
      bind(SegmentManager.class).in(LazySingleton.class);
      bind(QueryProcessingPool.class).toInstance(DirectQueryProcessingPool.INSTANCE);
      bindScope(LazySingleton.class, Scopes.SINGLETON);
    }

    @Provides
    @LazySingleton
    public List<StorageLocation> getLocations()
    {
      return locations;
    }
  }
}
