/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.loading.VirtualSegmentLoader;
import io.imply.druid.processing.Deferred;
import io.imply.druid.query.DeferredQueryRunner;
import io.imply.druid.segment.VirtualReferenceCountingSegment;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.ServerManager;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * This is {@link org.apache.druid.query.QuerySegmentWalker} used in historicals with virtual segments. It overrides
 * {@link ServerManager#buildQueryRunnerForSegment} so that download of virtual segment can be scheduled. The query runner
 * that is returned will wait on the download of segment before running the query on segment.
 */
public class DeferredLoadingQuerySegmentWalker extends ServerManager
{
  private VirtualSegmentLoader virtualSegmentLoader;

  @Inject
  public DeferredLoadingQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      @Deferred QueryProcessingPool queryProcessingPool,
      CachePopulator cachePopulator,
      @Smile ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig,
      SegmentManager segmentManager,
      JoinableFactory joinableFactory,
      ServerConfig serverConfig,
      VirtualSegmentLoader virtualSegmentLoader
  )
  {
    super(
        conglomerate,
        emitter,
        queryProcessingPool,
        cachePopulator,
        objectMapper,
        cache,
        cacheConfig,
        segmentManager,
        joinableFactory,
        serverConfig
    );
    this.virtualSegmentLoader = virtualSegmentLoader;
  }

  @Override
  protected <T> QueryRunner<T> buildQueryRunnerForSegment(
      final Query<T> query,
      final SegmentDescriptor descriptor,
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline,
      final Function<SegmentReference, SegmentReference> segmentMapFn,
      final AtomicLong cpuTimeAccumulator,
      Optional<byte[]> cacheKeyPrefix
  )
  {
    final PartitionChunk<ReferenceCountingSegment> chunk = timeline.findChunk(
        descriptor.getInterval(),
        descriptor.getVersion(),
        descriptor.getPartitionNumber()
    );

    if (chunk == null) {
      return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
    }

    final ReferenceCountingSegment segment = chunk.getObject();

    if (segment instanceof VirtualReferenceCountingSegment) {
      return buildQueryRunnerForVirtualSegment(
          query,
          descriptor,
          factory,
          toolChest,
          timeline,
          segmentMapFn,
          cpuTimeAccumulator,
          cacheKeyPrefix,
          (VirtualReferenceCountingSegment) segment
      );
    } else {
      return super.buildQueryRunnerForSegment(
          query,
          descriptor,
          factory,
          toolChest,
          timeline,
          segmentMapFn,
          cpuTimeAccumulator,
          cacheKeyPrefix
      );
    }
  }

  private <T> QueryRunner<T> buildQueryRunnerForVirtualSegment(
      final Query<T> query,
      final SegmentDescriptor descriptor,
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline,
      final Function<SegmentReference, SegmentReference> segmentMapFn,
      final AtomicLong cpuTimeAccumulator,
      Optional<byte[]> cacheKeyPrefix,
      VirtualReferenceCountingSegment virtualSegment
  )
  {
    ListenableFuture<Void> future = virtualSegmentLoader.scheduleDownload(virtualSegment);
    return virtualSegment.acquireReferences()
                         .map(closeable -> new DeferredQueryRunner<T>(
                             future,
                             virtualSegment,
                             closeable,
                             () -> super.buildQueryRunnerForSegment(
                                 query,
                                 descriptor,
                                 factory,
                                 toolChest,
                                 timeline,
                                 segmentMapFn,
                                 cpuTimeAccumulator,
                                 cacheKeyPrefix
                             )
                         ))
                         .orElseThrow(() -> new ISE("could not acquire the reference"));
  }
}
