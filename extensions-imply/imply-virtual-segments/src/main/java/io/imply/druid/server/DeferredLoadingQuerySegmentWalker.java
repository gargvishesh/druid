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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.loading.VirtualSegmentLoader;
import io.imply.druid.processing.Deferred;
import io.imply.druid.query.DeferredQueryRunner;
import io.imply.druid.segment.VirtualReferenceCountingSegment;
import io.imply.druid.segment.VirtualSegment;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.CPUTimeMetricQueryRunner;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.ServerManager;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * This is {@link org.apache.druid.query.QuerySegmentWalker} used in historicals with virtual segments. It is a copy of
 * {@link ServerManager} except following changes
 *  - overrides {@link ServerManager#getQueryRunnerForSegments(Query, Iterable)} to intercept segment metadata queries
 *  and handle them differently. Refer to {@link #buildQueryRunnerForSegmentMetadataQuery}
 *  - overrides {@link ServerManager#buildQueryRunnerForSegment} so that download of virtual segment can be scheduled.
 *  The query runner that is returned will wait on the download of segment before running the query on segment.
 */
public class DeferredLoadingQuerySegmentWalker extends ServerManager
{
  private static final EmittingLogger ALERT_LOG = new EmittingLogger(DeferredLoadingQuerySegmentWalker.class);
  private static final Logger LOG = new Logger(DeferredLoadingQuerySegmentWalker.class);
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ServiceEmitter emitter;
  private final QueryProcessingPool queryProcessingPool;
  private final SegmentManager segmentManager;
  private final JoinableFactoryWrapper joinableFactoryWrapper;
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
    this.conglomerate = conglomerate;
    this.emitter = emitter;
    this.queryProcessingPool = queryProcessingPool;
    this.virtualSegmentLoader = virtualSegmentLoader;
    this.segmentManager = segmentManager;
    this.joinableFactoryWrapper = new JoinableFactoryWrapper(joinableFactory);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      final QueryUnsupportedException e = new QueryUnsupportedException(
          StringUtils.format("Unknown query type, [%s]", query.getClass())
      );
      ALERT_LOG.makeAlert(e, "Error while executing a query[%s]", query.getId())
          .addData("dataSource", query.getDataSource())
          .emit();
      throw e;
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline;
    final Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> maybeTimeline =
        segmentManager.getTimeline(analysis);

    // Make sure this query type can handle the subquery, if present.
    if (analysis.isQuery() && !toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery())) {
      throw new ISE("Cannot handle subquery: %s", analysis.getDataSource());
    }

    if (maybeTimeline.isPresent()) {
      timeline = maybeTimeline.get();
    } else {
      return new ReportTimelineMissingSegmentQueryRunner<>(Lists.newArrayList(specs));
    }

    // segmentMapFn maps each base Segment into a joined Segment if necessary.
    final Function<SegmentReference, SegmentReference> segmentMapFn = joinableFactoryWrapper.createSegmentMapFn(
        analysis.getJoinBaseTableFilter().map(Filters::toFilter).orElse(null),
        analysis.getPreJoinableClauses(),
        cpuTimeAccumulator,
        analysis.getBaseQuery().orElse(query)
    );
    // We compute the join cache key here itself so it doesn't need to be re-computed for every segment
    final Optional<byte[]> cacheKeyPrefix = analysis.isJoin()
        ? joinableFactoryWrapper.computeJoinDataSourceCacheKey(analysis)
        : Optional.of(StringUtils.EMPTY_BYTES);

    // This is where the code diverges. We have to diverge the code here instead of inside `buildQueryRunnerForSegment`
    // since we need the information of all segments together to decide what segments should be downloaded
    FunctionalIterable<QueryRunner<T>> queryRunners;
    if (query.getType().equals(Query.SEGMENT_METADATA)) {
      queryRunners = buildQueryRunnerForSegmentMetadataQuery(
          specs,
          query,
          factory,
          toolChest,
          timeline,
          segmentMapFn,
          cpuTimeAccumulator,
          cacheKeyPrefix);
    } else {
      queryRunners = FunctionalIterable
          .create(specs)
          .transformCat(
              descriptor -> Collections.singletonList(
                  buildQueryRunnerForSegment(
                      query,
                      descriptor,
                      factory,
                      toolChest,
                      timeline,
                      segmentMapFn,
                      cpuTimeAccumulator,
                      cacheKeyPrefix
                  )
              )
          );
    }
    return CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(queryProcessingPool, queryRunners)),
            toolChest
        ),
        toolChest,
        emitter,
        cpuTimeAccumulator,
        true
    );
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

    final VirtualReferenceCountingSegment segment = (VirtualReferenceCountingSegment) chunk.getObject();

    return buildQueryRunnerForVirtualSegment(
        query,
        descriptor,
        factory,
        toolChest,
        timeline,
        segmentMapFn,
        cpuTimeAccumulator,
        cacheKeyPrefix,
        segment
    );

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
    Closer closer = Closer.create();
    ListenableFuture<Void> future = virtualSegmentLoader.scheduleDownload(virtualSegment, closer);
    return new DeferredQueryRunner<T>(
        future,
        virtualSegment,
        closer,
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
    );
  }

  /**
   * Builds a QueryRunner object for segment metadata query such that not all segments need to be downloaded. It
   * samples few segments and schedules them for download. For rest of the segments, dummy results are returned. we need
   * to return dummy results to not mess up the result ordering. If we do not return any result for some specs, it might
   * surface as an error on the broker side. Broker may also keep retrying pulling metadata for the skipped segments that
   * we do not want. Dummy metadata for these segments is fine since any queries for showing console will
   * always land on hot brokers. Hot brokers are not aware of cold segments and they will not issue metadata queries
   * for the cold segments anyway.
   */
  private <T> FunctionalIterable<QueryRunner<T>> buildQueryRunnerForSegmentMetadataQuery(
      Iterable<SegmentDescriptor> specs,
      Query<T> query,
      QueryRunnerFactory<T, Query<T>> factory,
      QueryToolChest<T, Query<T>> toolChest,
      VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline,
      Function<SegmentReference, SegmentReference> segmentMapFn,
      AtomicLong cpuTimeAccumulator,
      Optional<byte[]> cacheKeyPrefix)
  {
    Set<SegmentDescriptor> queryableSpecs = segmentsToQueryForMetadata(specs);
    return FunctionalIterable
        .create(specs)
        .transformCat(
            descriptor -> {
              if (queryableSpecs.contains(descriptor)) {
                return Collections.singletonList(
                    buildQueryRunnerForSegment(
                        query,
                        descriptor,
                        factory,
                        toolChest,
                        timeline,
                        segmentMapFn,
                        cpuTimeAccumulator,
                        cacheKeyPrefix
                    )
                );
              } else {
                return Collections.singletonList(
                    buildDummyRunnerForSegmentMetadata(
                        descriptor,
                        timeline
                    )
                );
              }
            }
        );
  }

  /**
   * Sample the segment descriptors. It returns the latest and oldest segment in the specs. The rationale in picking two
   * segments is that it gives a good spread as opposed to picking just one segment. If we just pick one segment, say the
   * latest one, it is possible that the schema of that segment is very different from existing segments. The datasource
   * signature on the cold broker could change drastically because of just one segment. By including the oldest segment
   * as well, the impact is minimized to a certain extent. It is not very perfect either but will do for now.
   * @param specs - segment descriptors to sample
   */
  @VisibleForTesting
  static Set<SegmentDescriptor> segmentsToQueryForMetadata(Iterable<SegmentDescriptor> specs)
  {
    SegmentDescriptor oldestSpec = null;
    SegmentDescriptor newestSpec = null;
    Interval minInterval = null;
    Interval maxInterval = null;
    Comparator<Interval> comparator = Comparators.intervalsByEndThenStart();
    for (SegmentDescriptor spec : specs) {
      Interval interval = spec.getInterval();
      if (minInterval == null || comparator.compare(minInterval, interval) > 0) {
        minInterval = interval;
        oldestSpec = spec;
      }
      if (maxInterval == null || comparator.compare(interval, maxInterval) > 0) {
        maxInterval = interval;
        newestSpec = spec;
      }
    }
    return Sets.newHashSet(oldestSpec, newestSpec);
  }

  private <T> QueryRunner<T> buildDummyRunnerForSegmentMetadata(SegmentDescriptor descriptor, VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline)
  {
    final PartitionChunk<ReferenceCountingSegment> chunk = timeline.findChunk(
        descriptor.getInterval(),
        descriptor.getVersion(),
        descriptor.getPartitionNumber()
    );

    if (chunk == null) {
      return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
    }

    final VirtualReferenceCountingSegment segment = (VirtualReferenceCountingSegment) chunk.getObject();

    final QueryRunner<T> delegate = (queryPlus, responseContext) -> {
      DataSegment dataSegment = ((VirtualSegment) segment.getBaseSegment()).asDataSegment();
      return (Sequence<T>) Sequences.simple(
          Collections.singletonList(
              new SegmentAnalysis(
                  dataSegment.getId().toString(),
                  Collections.singletonList(dataSegment.getInterval()),
                  new LinkedHashMap<>(),
                  dataSegment.getSize(),
                  -1,
                  Collections.emptyMap(),
                  null,
                  null,
                  false
              )
          )
      );
    };

    // Following wrapper is required since it changes the result type when bySegment field is set to true
    // Broker expects row of to be of type `Result` instead of `SegmentAnalysis`
    BySegmentQueryRunner<T> bySegmentQueryRunner = new BySegmentQueryRunner<>(
        segment.getId(),
        segment.getDataInterval().getStart(),
        delegate
    );

    // Return a query runner of type DeferredQueryRunner
    return new DeferredQueryRunner<T>(
        Futures.immediateFuture(null),
        segment,
        () -> {
        },
        () -> bySegmentQueryRunner);
  }
}
