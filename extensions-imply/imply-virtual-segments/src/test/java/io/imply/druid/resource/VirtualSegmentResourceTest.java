/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import io.imply.druid.TestData;
import io.imply.druid.VirtualSegmentConfig;
import io.imply.druid.loading.FIFOSegmentReplacementStrategy;
import io.imply.druid.loading.VirtualSegmentLoader;
import io.imply.druid.segment.VirtualReferenceCountingSegment;
import io.imply.druid.segment.VirtualSegment;
import io.imply.druid.segment.VirtualSegmentStateManagerImpl;
import io.imply.druid.segment.VirtualSegmentStats;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class VirtualSegmentResourceTest
{
  private SegmentLoaderConfig loaderConfig;
  private VirtualSegmentConfig config;
  private SegmentManager segmentManager;


  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();
  private ObjectMapper jsonMapper;
  private SegmentLocalCacheManager physicalManager;
  private SegmentizerFactory segmentizerFactory;


  @Before
  public void setup() throws IOException
  {
    Injector injector = GuiceInjectors.makeStartupInjector();
    jsonMapper = injector.getInstance(ObjectMapper.class);

    config = new VirtualSegmentConfig(1, 1L);
    loaderConfig = new SegmentLoaderConfig().withLocations(Collections.singletonList(new StorageLocationConfig(
        tempFolder.newFolder(),
        10_000L,
        0.0d
    )));


    physicalManager = Mockito.mock(SegmentLocalCacheManager.class);
    segmentizerFactory = Mockito.mock(SegmentizerFactory.class);
    segmentManager = Mockito.mock(SegmentManager.class);
  }

  @Test
  public void deleteSegmentsSanity() throws IOException
  {
    Closer closer = Closer.create();
    VirtualReferenceCountingSegment firstSegment = TestData.buildVirtualSegment(1);

    VirtualSegmentStats virtualSegmentStats = new VirtualSegmentStats();
    VirtualSegmentStateManagerImpl virtualSegmentHolder = new VirtualSegmentStateManagerImpl(
        new FIFOSegmentReplacementStrategy(), virtualSegmentStats

    );
    VirtualSegmentResource virtualSegmentResource = createVirtualSegmentResource(
        virtualSegmentHolder,
        virtualSegmentStats
    );

    virtualSegmentHolder.registerIfAbsent(firstSegment);
    ListenableFuture<Void> future = virtualSegmentHolder.queue(firstSegment, closer);
    virtualSegmentHolder.downloaded(firstSegment);

    Mockito.when(segmentManager.getDataSourceNames()).thenReturn(new HashSet<String>()
    {
      {
        add(firstSegment.getId().getDataSource());
      }
    });

    //Closing the reference so that segment can be evicted.
    closer.close();

    VersionedIntervalTimeline<String, ReferenceCountingSegment> versionedIntervalTimeline = new VersionedIntervalTimeline(
        Ordering.natural());

    Mockito.when(segmentManager.getTimeline(ArgumentMatchers.any(DataSourceAnalysis.class)))
           .thenReturn(Optional.of(versionedIntervalTimeline));
    versionedIntervalTimeline.add(
        firstSegment.getDataInterval(),
        firstSegment.getId().getVersion(),
        ((VirtualSegment) firstSegment.getBaseSegment()).asDataSegment().getShardSpec().createChunk(firstSegment)
    );

    Assert.assertTrue(future.isDone());

    Response response = virtualSegmentResource.evictVirtualSegments();

    Assert.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    Assert.assertEquals(1, virtualSegmentStats.getNumSegmentsEvicted());
    final Map<String, List<String>> expectedResult = new HashMap();
    expectedResult.put(firstSegment.getId().getDataSource(), new ArrayList<String>()
    {
      {
        add(firstSegment.getId().toString());
      }
    });
    Assert.assertEquals(expectedResult, jsonMapper.readValue(response.getEntity().toString(), HashMap.class));
  }

  @Test
  public void queryInProgress() throws JsonProcessingException
  {
    //as we are mocking query in progres, we will not close the closer.
    Closer closer = Closer.create();
    VirtualReferenceCountingSegment firstSegment = TestData.buildVirtualSegment(1);
    VirtualSegmentStats virtualSegmentStats = new VirtualSegmentStats();
    VirtualSegmentStateManagerImpl virtualSegmentHolder = new VirtualSegmentStateManagerImpl(
        new FIFOSegmentReplacementStrategy(), virtualSegmentStats

    );
    VirtualSegmentResource virtualSegmentResource = createVirtualSegmentResource(
        virtualSegmentHolder,
        virtualSegmentStats
    );

    virtualSegmentHolder.registerIfAbsent(firstSegment);
    ListenableFuture<Void> future = virtualSegmentHolder.queue(firstSegment, closer);
    virtualSegmentHolder.downloaded(firstSegment);

    Mockito.when(segmentManager.getDataSourceNames()).thenReturn(
        new HashSet<String>()
        {
          {
            add(firstSegment.getId().getDataSource());
          }
        });


    VersionedIntervalTimeline<String, ReferenceCountingSegment> versionedIntervalTimeline = new VersionedIntervalTimeline(
        Ordering.natural());

    Mockito.when(segmentManager.getTimeline(ArgumentMatchers.any(DataSourceAnalysis.class)))
           .thenReturn(Optional.of(versionedIntervalTimeline));
    versionedIntervalTimeline.add(
        firstSegment.getDataInterval(),
        firstSegment.getId().getVersion(),
        ((VirtualSegment) firstSegment.getBaseSegment()).asDataSegment().getShardSpec().createChunk(firstSegment)
    );

    Assert.assertTrue(future.isDone());

    Response response = virtualSegmentResource.evictVirtualSegments();

    Assert.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    Assert.assertEquals(0, virtualSegmentStats.getNumSegmentsEvicted());
    Assert.assertEquals(new HashMap(), jsonMapper.readValue(response.getEntity().toString(), HashMap.class)
    );
  }

  private VirtualSegmentResource createVirtualSegmentResource(
      VirtualSegmentStateManagerImpl virtualSegmentHolder,
      VirtualSegmentStats stats
  )
  {
    VirtualSegmentLoader virtualSegmentLoader = new VirtualSegmentLoader(
        physicalManager,
        loaderConfig,
        virtualSegmentHolder,
        config,
        null,
        segmentizerFactory,
        stats,
        new ScheduledThreadPoolExecutor(1)
    );
    virtualSegmentLoader.start();
    return new VirtualSegmentResource(segmentManager, virtualSegmentLoader, jsonMapper);
  }
}
