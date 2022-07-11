/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import io.imply.druid.query.samplinggroupby.metrics.DefaultSamplingGroupByQueryMetricsFactory;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.having.GreaterThanHavingSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class SamplingGroupByMultiSegmentTest
{
  public static final ObjectMapper JSON_MAPPER;

  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;

  private File tmpDir;
  private QueryRunnerFactory<ResultRow, SamplingGroupByQuery> samplingGroupByFactory;
  private final List<IncrementalIndex> incrementalIndices = new ArrayList<>();
  private List<QueryableIndex> samplingGroupByIndices = new ArrayList<>();
  private ExecutorService executorService;
  private Closer resourceCloser;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(
        new InjectableValues.Std().addValue(
            ExprMacroTable.class,
            ExprMacroTable.nil()
        )
    );
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
    NullHandling.initializeForTests();
  }


  private IncrementalIndex makeIncIndex(boolean withRollup)
  {
    return new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(new DimensionsSpec(
                    Arrays.asList(
                        new StringDimensionSchema("dimA"),
                        new LongDimensionSchema("metA")
                    )
                ))
                .withRollup(withRollup)
                .build()
        )
        .setConcurrentEventAdd(true)
        .setMaxRowCount(1000)
        .build();
  }

  @Before
  public void setup() throws Exception
  {
    tmpDir = FileUtils.createTempDir();

    InputRow row;
    List<String> dimNames = Arrays.asList("dimA", "metA");
    Map<String, Object> event;

    final IncrementalIndex indexA = makeIncIndex(false);
    incrementalIndices.add(indexA);
    event = new HashMap<>();
    event.put("dimA", "hello");
    event.put("metA", 100);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);
    event = new HashMap<>();
    event.put("dimA", "world");
    event.put("metA", 75);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexA.add(row);
    final File fileA = INDEX_MERGER_V9.persist(
        indexA,
        new File(tmpDir, "A"),
        new IndexSpec(),
        null
    );
    QueryableIndex qindexA = INDEX_IO.loadIndex(fileA);


    final IncrementalIndex indexB = makeIncIndex(false);
    incrementalIndices.add(indexB);
    event = new HashMap<>();
    event.put("dimA", "foo");
    event.put("metA", 100);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexB.add(row);
    event = new HashMap<>();
    event.put("dimA", "world");
    event.put("metA", 75);
    row = new MapBasedInputRow(1000, dimNames, event);
    indexB.add(row);

    final File fileB = INDEX_MERGER_V9.persist(
        indexB,
        new File(tmpDir, "B"),
        new IndexSpec(),
        null
    );
    QueryableIndex qindexB = INDEX_IO.loadIndex(fileB);

    samplingGroupByIndices = Arrays.asList(qindexA, qindexB);
    resourceCloser = Closer.create();
    setupGroupByFactory();
  }

  private void setupGroupByFactory()
  {
    executorService = Execs.multiThreaded(2, "SamplingGroupByThreadPool[%d]");

    CloseableStupidPool<ByteBuffer> bufferPool = new CloseableStupidPool<>(
        "SamplingGroupByBenchmark-computeBufferPool",
        new OffheapBufferGenerator("compute", 10_000_000),
        0,
        Integer.MAX_VALUE
    );

    resourceCloser.register(bufferPool);

    samplingGroupByFactory = new SamplingGroupByQueryRunnerFactory(
        new SamplingGroupByQueryToolChest(DefaultSamplingGroupByQueryMetricsFactory.instance()),
        GroupByQueryRunnerTest.DEFAULT_PROCESSING_CONFIG,
        bufferPool,
        NOOP_QUERYWATCHER
    );
  }

  @After
  public void tearDown() throws Exception
  {
    for (IncrementalIndex incrementalIndex : incrementalIndices) {
      incrementalIndex.close();
    }

    for (QueryableIndex queryableIndex : samplingGroupByIndices) {
      queryableIndex.close();
    }

    resourceCloser.close();

    if (tmpDir != null) {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Test
  public void testMultipleSegmentsWithBothQueryableIndex()
  {
    QueryToolChest<ResultRow, SamplingGroupByQuery> toolChest = samplingGroupByFactory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.postMergeQueryDecoration(toolChest.mergeResults(
            samplingGroupByFactory.mergeRunners(executorService, makeGroupByMultiRunners(samplingGroupByIndices, ImmutableList.of()))
        )),
        (QueryToolChest) toolChest
    );
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(0, 1000000))
    );

    SamplingGroupByQuery query = new SamplingGroupByQuery(
        new TableDataSource("blah"),
        intervalSpec,
        ImmutableList.of(new DefaultDimensionSpec("dimA", null)),
        ImmutableList.of(new CountAggregatorFactory("count")),
        ImmutableList.of(new FieldAccessPostAggregator("fieldAccessPostAgg", "_samplingRate")),
        new OrDimFilter(
            new SelectorDimFilter("dimA", "world", null, null),
            new SelectorDimFilter("dimA", "hello", null, null)
        ),
        new GreaterThanHavingSpec("count", 1),
        new AllGranularity(),
        new HashMap<>(),
        10
    );

    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();

    ResultRow[] expectedRows = new ResultRow[]{
        ResultRow.of(
            "world",
            1.0D,
            2L,
            1.0D
        )
    };

    Assert.assertEquals(1, results.size());
    Assert.assertArrayEquals(expectedRows, results.toArray(new ResultRow[0]));
  }

  @Test
  public void testMultipleSegmentsWithOneQueryableIndexAndOneIncementalIndex()
  {
    QueryToolChest<ResultRow, SamplingGroupByQuery> toolChest = samplingGroupByFactory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            samplingGroupByFactory.mergeRunners(executorService, makeGroupByMultiRunners(
                ImmutableList.of(samplingGroupByIndices.get(1)),
                ImmutableList.of(incrementalIndices.get(0))
            ))
        ),
        (QueryToolChest) toolChest
    );
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(0, 1000000))
    );

    SamplingGroupByQuery query = new SamplingGroupByQuery(
        new TableDataSource("blah"),
        intervalSpec,
        ImmutableList.of(new DefaultDimensionSpec("dimA", null)),
        ImmutableList.of(new CountAggregatorFactory("count")),
        ImmutableList.of(),
        null,
        null,
        new AllGranularity(),
        new HashMap<>(),
        10
    );

    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();

    ResultRow[] expectedRows = new ResultRow[]{
        ResultRow.of(
            "hello",
            1.0D,
            1L
        ),
        ResultRow.of(
            "world",
            1.0D,
            2L
        ),
        ResultRow.of(
            "foo",
            1.0D,
            1L
        )
    };

    Assert.assertEquals(3, results.size());
    Assert.assertArrayEquals(expectedRows, results.toArray(new ResultRow[0]));
  }

  @Test
  public void testMultipleSegmentsWithBothIncementalIndex()
  {
    QueryToolChest<ResultRow, SamplingGroupByQuery> toolChest = samplingGroupByFactory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            samplingGroupByFactory.mergeRunners(executorService, makeGroupByMultiRunners(
                ImmutableList.of(),
                ImmutableList.of(incrementalIndices.get(0), incrementalIndices.get(1))
            ))
        ),
        (QueryToolChest) toolChest
    );
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(Intervals.utc(0, 1000000))
    );

    SamplingGroupByQuery query = new SamplingGroupByQuery(
        new TableDataSource("blah"),
        intervalSpec,
        ImmutableList.of(new DefaultDimensionSpec("dimA", null)),
        ImmutableList.of(new CountAggregatorFactory("count")),
        ImmutableList.of(),
        null,
        null,
        new AllGranularity(),
        new HashMap<>(),
        10
    );

    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();

    ResultRow[] expectedRows = new ResultRow[]{
        ResultRow.of(
            "hello",
            1.0D,
            1L
        ),
        ResultRow.of(
            "world",
            1.0D,
            2L
        ),
        ResultRow.of(
            "foo",
            1.0D,
            1L
        )
    };

    Assert.assertEquals(3, results.size());
    Assert.assertArrayEquals(expectedRows, results.toArray(new ResultRow[0]));
  }

  private List<QueryRunner<ResultRow>> makeGroupByMultiRunners(
      List<QueryableIndex> queryableIndices,
      List<IncrementalIndex> incrementalIndices
  )
  {
    ImmutableList.Builder<QueryRunner<ResultRow>> runnersBuilder = ImmutableList.builder();

    for (QueryableIndex qindex : queryableIndices) {
      QueryRunner<ResultRow> runner = makeQueryRunner(
          samplingGroupByFactory,
          SegmentId.dummy(qindex.toString()),
          new QueryableIndexSegment(qindex, SegmentId.dummy(qindex.toString()))
      );
      runnersBuilder.add(samplingGroupByFactory.getToolchest().preMergeQueryDecoration(runner));
    }

    for (IncrementalIndex incrementalIndex : incrementalIndices) {
      QueryRunner<ResultRow> runner = makeQueryRunner(
          samplingGroupByFactory,
          SegmentId.dummy(incrementalIndex.toString()),
          new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy(incrementalIndex.toString()))
      );
      runnersBuilder.add(samplingGroupByFactory.getToolchest().preMergeQueryDecoration(runner));
    }
    return runnersBuilder.build();
  }

  private static class OffheapBufferGenerator implements Supplier<ByteBuffer>
  {
    private static final Logger log = new Logger(OffheapBufferGenerator.class);

    private final String description;
    private final int computationBufferSize;
    private final AtomicLong count = new AtomicLong(0);

    public OffheapBufferGenerator(String description, int computationBufferSize)
    {
      this.description = description;
      this.computationBufferSize = computationBufferSize;
    }

    @Override
    public ByteBuffer get()
    {
      log.info(
          "Allocating new %s buffer[%,d] of size[%,d]",
          description,
          count.getAndIncrement(),
          computationBufferSize
      );

      return ByteBuffer.allocateDirect(computationBufferSize);
    }
  }

  public static <T, QueryType extends Query<T>> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, QueryType> factory,
      SegmentId segmentId,
      Segment adapter
  )
  {
    return new FinalizeResultsQueryRunner<>(
        new BySegmentQueryRunner<>(segmentId, adapter.getDataInterval().getStart(), factory.createRunner(adapter)),
        (QueryToolChest<T, Query<T>>) factory.getToolchest()
    );
  }

  public static final QueryWatcher NOOP_QUERYWATCHER = (query, future) -> {};
}
