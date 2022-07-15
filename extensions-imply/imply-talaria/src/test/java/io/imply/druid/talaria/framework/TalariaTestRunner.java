/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.framework;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Providers;
import io.imply.druid.storage.LocalFileStorageConnectorProvider;
import io.imply.druid.storage.StorageConnector;
import io.imply.druid.storage.StorageConnectorProvider;
import io.imply.druid.talaria.frame.testutil.FrameTestUtil;
import io.imply.druid.talaria.guice.Talaria;
import io.imply.druid.talaria.guice.TalariaIndexingModule;
import io.imply.druid.talaria.guice.TalariaSqlModule;
import io.imply.druid.talaria.indexing.DataSourceMSQDestination;
import io.imply.druid.talaria.indexing.TalariaQuerySpec;
import io.imply.druid.talaria.indexing.error.InsertLockPreemptedFaultTest;
import io.imply.druid.talaria.indexing.error.MSQErrorReport;
import io.imply.druid.talaria.indexing.error.TalariaFault;
import io.imply.druid.talaria.indexing.report.TalariaResultsReport;
import io.imply.druid.talaria.indexing.report.TalariaTaskReportPayload;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import io.imply.druid.talaria.querykit.LazyResourceHolder;
import io.imply.druid.talaria.sql.ImplyQueryMakerFactory;
import io.imply.druid.talaria.sql.TalariaQueryMaker;
import io.imply.druid.talaria.util.TalariaContext;
import org.apache.calcite.tools.RelConversionException;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.parser.DruidSqlParserUtils;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.view.InProcessViewManager;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.PruneLoadSpec;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE1;
import static org.apache.druid.sql.calcite.util.CalciteTests.DATASOURCE2;
import static org.apache.druid.sql.calcite.util.CalciteTests.ROWS1;
import static org.apache.druid.sql.calcite.util.CalciteTests.ROWS2;

/**
 * Base test runner for running talaria unit tests. It sets up talaria query execution environment
 * and populates data for the datasources. The runner does not go via the HTTP layer for communication between the
 * various talaria processes.
 * <p>
 * Leader -> Coordinator (Coordinator is mocked)
 * <p>
 * In the Ut's we go from:
 * {@link TalariaQueryMaker} -> {@link TalariaTestOverlordServiceClient} -> {@link io.imply.druid.talaria.exec.Leader}
 * <p>
 * <p>
 * Leader -> Worker communication happens in {@link TalariaTestLeaderContext}
 * <p>
 * Worker -> Leader communication happens in {@link TalariaTestLeaderClient}
 * <p>
 * Leader -> Overlord communication happens in {@link TalariaTestTaskActionClient}
 */
public class TalariaTestRunner extends BaseCalciteQueryTest
{

  public static final Map<String, Object> DEFAULT_TALARIA_CONTEXT = ImmutableMap.<String, Object>builder()
                                                                                .put("multiStageQuery", true)
                                                                                .put(
                                                                                    TalariaContext.CTX_DURABLE_SHUFFLE_STORAGE,
                                                                                    true
                                                                                )
                                                                                .build();

  public static final Map<String, Object>
      REPLACE_TIME_CHUCKS_CONTEXT = ImmutableMap.<String, Object>builder()
                                                .putAll(DEFAULT_TALARIA_CONTEXT)
                                                .put(DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS, DruidSqlParserUtils.ALL)
                                                .build();

  public static final Map<String, Object>
      ROLLUP_CONTEXT = ImmutableMap.<String, Object>builder()
                                   .putAll(DEFAULT_TALARIA_CONTEXT)
                                   .put(TalariaContext.CTX_FINALIZE_AGGREGATIONS, false)
                                   .put(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false)
                                   .build();

  public final boolean useDefault = NullHandling.replaceWithDefault();

  protected File localFileStorageDir;
  private static final Logger log = new Logger(TalariaTestRunner.class);
  private PlannerFactory plannerFactory;
  private ObjectMapper objectMapper;
  private TalariaTestOverlordServiceClient indexingServiceClient;
  private SqlLifecycleFactory sqlLifeCycleFactory;
  private IndexIO indexIO;

  private TalariaTestSegmentManager segmentManager;
  private SegmentCacheManager segmentCacheManager;
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private TestGroupByBuffers groupByBuffers;

  @After
  public void tearDown2()
  {
    groupByBuffers.close();
  }

  @Before
  public void setUp2()
  {
    Injector secondInjector = GuiceInjectors.makeStartupInjector();

    groupByBuffers = TestGroupByBuffers.createDefault();

    ObjectMapper secondMapper = setupObjectMapper(secondInjector);
    indexIO = new IndexIO(secondMapper, () -> 0);

    try {
      segmentCacheManager = new SegmentCacheManagerFactory(secondMapper).manufacturate(temporaryFolder.newFolder("test"));
    }
    catch (IOException exception) {
      throw new ISE(exception, "Unable to create segmentCacheManager");
    }

    TalariaSqlModule sqlModule = new TalariaSqlModule();

    segmentManager = new TalariaTestSegmentManager(segmentCacheManager, indexIO);

    Injector injector = GuiceInjectors.makeStartupInjectorWithModules(ImmutableList.of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
            {
              @Override
              public String getFormatString()
              {
                return "test";
              }
            };

            GroupByQueryConfig groupByQueryConfig = new GroupByQueryConfig();

            binder.bind(DruidProcessingConfig.class).toInstance(druidProcessingConfig);
            binder.bind(new TypeLiteral<Set<NodeRole>>()
            {
            }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.PEON));
            binder.bind(QueryProcessingPool.class)
                  .toInstance(new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool")));
            binder.bind(DataSegmentProvider.class)
                  .toInstance((dataSegment, channelCounters) ->
                                  new LazyResourceHolder<>(getSupplierForSegment(dataSegment)));
            binder.bind(IndexIO.class).toInstance(indexIO);
            binder.bind(JoinableFactory.class).toInstance(NoopJoinableFactory.INSTANCE);
            binder.bind(SpecificSegmentsQuerySegmentWalker.class).toInstance(walker);

            binder.bind(GroupByStrategySelector.class)
                  .toInstance(GroupByQueryRunnerTest.makeQueryRunnerFactory(groupByQueryConfig, groupByBuffers)
                                                    .getStrategySelector());

            LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
            try {
              config.storageDirectory = tmpFolder.newFolder("localsegments");
            }
            catch (IOException e) {
              throw new ISE(e, "Unable to create folder");
            }
            binder.bind(DataSegmentPusher.class).toInstance(new TalariaTestDelegateDataSegmentPusher(
                new LocalDataSegmentPusher(config),
                segmentManager
            ));
            binder.bind(DataSegmentAnnouncer.class).toInstance(new NoopDataSegmentAnnouncer());
            binder.bindConstant().annotatedWith(PruneLoadSpec.class).to(false);
            // Client is not used in tests
            binder.bind(Key.get(ServiceClientFactory.class, EscalatedGlobal.class))
                  .toProvider(Providers.of(null));
            // fault tolerance module
            try {
              JsonConfigProvider.bind(
                  binder,
                  TalariaIndexingModule.TALARIA_INTERMEDIATE_STORAGE,
                  StorageConnectorProvider.class,
                  Talaria.class
              );
              localFileStorageDir = tmpFolder.newFolder("fault");
              binder.bind(Key.get(StorageConnector.class, Talaria.class))
                    .toProvider(new LocalFileStorageConnectorProvider(localFileStorageDir.toURI().getPath()));
            }
            catch (IOException e) {
              throw new ISE(e, "Unable to create setup storage connector");
            }
          }
        },
        new IndexingServiceTuningConfigModule(),
        new TalariaIndexingModule(),
        sqlModule

    ));

    objectMapper = setupObjectMapper(injector);
    objectMapper.registerModules(sqlModule.getJacksonModules());

    indexingServiceClient = new TalariaTestOverlordServiceClient(
        objectMapper,
        injector,
        new TalariaTestTaskActionClient(objectMapper)
    );
    final InProcessViewManager viewManager = new InProcessViewManager(CalciteTests.DRUID_VIEW_MACRO_FACTORY);
    DruidSchemaCatalog rootSchema = CalciteTests.createMockRootSchema(
        conglomerate,
        walker,
        new PlannerConfig(),
        viewManager,
        new NoopDruidSchemaManager(),
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );

    this.plannerFactory = new PlannerFactory(
        rootSchema,
        new ImplyQueryMakerFactory(
            CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
            indexingServiceClient,
            queryJsonMapper.copy().registerModules(new TalariaSqlModule().getJacksonModules())
        ),
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        PLANNER_CONFIG_DEFAULT,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        objectMapper,
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of())
    );

    sqlLifeCycleFactory = CalciteTests.createSqlLifecycleFactory(plannerFactory);

  }

  /**
   * Helper method that copies a resource to a temporary file, then returns it.
   */
  protected File getResourceAsTemporaryFile(final String resource) throws IOException
  {
    final File file = temporaryFolder.newFile();
    final InputStream stream = getClass().getResourceAsStream(resource);

    if (stream == null) {
      throw new IOE("No such resource [%s]", resource);
    }

    ByteStreams.copy(stream, Files.newOutputStream(file.toPath()));
    return file;
  }

  @Nonnull
  private Supplier<Pair<Segment, Closeable>> getSupplierForSegment(SegmentId segmentId)
  {
    if (segmentManager.getSegment(segmentId) == null) {
      final QueryableIndex index;
      TemporaryFolder temporaryFolder = new TemporaryFolder();
      try {
        temporaryFolder.create();
      }
      catch (IOException e) {
        throw new ISE(e, "Unable to create temporary folder for tests");
      }
      try {
        switch (segmentId.getDataSource()) {
          case DATASOURCE1:
            IncrementalIndexSchema foo1Schema = new IncrementalIndexSchema.Builder()
                .withMetrics(
                    new CountAggregatorFactory("cnt"),
                    new FloatSumAggregatorFactory("m1", "m1"),
                    new DoubleSumAggregatorFactory("m2", "m2"),
                    new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                )
                .withRollup(false)
                .build();
            index = IndexBuilder
                .create()
                .tmpDir(new File(temporaryFolder.newFolder(), "1"))
                .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                .schema(foo1Schema)
                .rows(ROWS1)
                .buildMMappedIndex();
            break;
          case DATASOURCE2:
            final IncrementalIndexSchema indexSchemaDifferentDim3M1Types = new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(
                    new DimensionsSpec(
                        ImmutableList.of(
                            new StringDimensionSchema("dim1"),
                            new StringDimensionSchema("dim2"),
                            new LongDimensionSchema("dim3")
                        )
                    )
                )
                .withMetrics(
                    new CountAggregatorFactory("cnt"),
                    new LongSumAggregatorFactory("m1", "m1"),
                    new DoubleSumAggregatorFactory("m2", "m2"),
                    new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                )
                .withRollup(false)
                .build();
            index = IndexBuilder
                .create()
                .tmpDir(new File(temporaryFolder.newFolder(), "1"))
                .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                .schema(indexSchemaDifferentDim3M1Types)
                .rows(ROWS2)
                .buildMMappedIndex();
            break;
          default:
            throw new ISE("Cannot query segment %s in test runner", segmentId);

        }
      }
      catch (IOException e) {
        throw new ISE(e, "Unable to load index for segment %s", segmentId);
      }
      Segment segment = new Segment()
      {
        @Override
        public SegmentId getId()
        {
          return segmentId;
        }

        @Override
        public Interval getDataInterval()
        {
          return segmentId.getInterval();
        }

        @Nullable
        @Override
        public QueryableIndex asQueryableIndex()
        {
          return index;
        }

        @Override
        public StorageAdapter asStorageAdapter()
        {
          return new QueryableIndexStorageAdapter(index);
        }

        @Override
        public void close()
        {
        }
      };
      segmentManager.addSegment(segment);
    }
    return new Supplier<Pair<Segment, Closeable>>()
    {
      @Override
      public Pair<Segment, Closeable> get()
      {
        return new Pair<>(segmentManager.getSegment(segmentId), Closer.create());
      }
    };
  }

  public SelectQueryTester testSelectQuery()
  {
    return new SelectQueryTester();
  }

  public IngestQueryTester testIngestQuery()
  {
    return new IngestQueryTester();
  }

  private ObjectMapper setupObjectMapper(Injector injector)
  {
    ObjectMapper mapper = injector.getInstance(ObjectMapper.class)
                                  .registerModules(new SimpleModule(IndexingServiceTuningConfigModule.class.getSimpleName())
                                                       .registerSubtypes(
                                                           new NamedType(IndexTask.IndexTuningConfig.class, "index"),
                                                           new NamedType(
                                                               ParallelIndexTuningConfig.class,
                                                               "index_parallel"
                                                           ),
                                                           new NamedType(
                                                               CompactionTask.CompactionTuningConfig.class,
                                                               "compaction"
                                                           )
                                                       ));
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ObjectMapper.class, mapper)
            .addValue(Injector.class, injector)
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
            .addValue(LocalDataSegmentPuller.class, new LocalDataSegmentPuller())
            .addValue(ExprMacroTable.class, CalciteTests.createExprMacroTable())
    );

    mapper.registerSubtypes(new NamedType(LocalLoadSpec.class, "local"));

    // This should be reusing guice instead of using static classes
    InsertLockPreemptedFaultTest.LockPreemptedHelper.preempt(false);

    return mapper;
  }

  private String runTalariaQuery(String query, Map<String, Object> context) throws RelConversionException
  {
    SqlLifecycle lifecycle = sqlLifeCycleFactory.factorize();
    final List<Object[]> sequence = lifecycle.runSimple(
        query,
        context,
        ImmutableList.of(),
        CalciteTests.REGULAR_USER_AUTH_RESULT
    ).toList();

    return (String) Iterables.getOnlyElement(sequence)[0];
  }

  private boolean planQueryOnly(String sql, Map<String, Object> queryContext) throws RelConversionException
  {
    SqlLifecycle lifecycle = sqlLifeCycleFactory.factorize();
    lifecycle.initialize(sql, new QueryContext(queryContext));
    lifecycle.setParameters(SqlQuery.getParameterList(ImmutableList.of()));
    lifecycle.validateAndAuthorize(CalciteTests.REGULAR_USER_AUTH_RESULT);
    lifecycle.plan();
    return true;
  }

  private TalariaTaskReportPayload getPayloadOrThrow(String controllerTaskId)
  {
    TalariaTaskReportPayload payload =
        (TalariaTaskReportPayload) indexingServiceClient.getReportForTask(controllerTaskId)
                                                        .get("multiStageQuery")
                                                        .getPayload();
    if (payload.getStatus().getStatus().isFailure()) {
      throw new ISE(
          "Query task [%s] failed due to %s",
          controllerTaskId,
          payload.getStatus().getErrorReport().toString()
      );
    }

    if (!payload.getStatus().getStatus().isComplete()) {
      throw new ISE("Query task [%s] should have finished", controllerTaskId);
    }

    return payload;
  }

  private MSQErrorReport getErrorReportOrThrow(String controllerTaskId)
  {
    TalariaTaskReportPayload payload =
        (TalariaTaskReportPayload) indexingServiceClient.getReportForTask(controllerTaskId)
                                                        .get("multiStageQuery")
                                                        .getPayload();
    if (!payload.getStatus().getStatus().isFailure()) {
      throw new ISE(
          "Query task [%s] was supposed to fail",
          controllerTaskId
      );
    }

    if (!payload.getStatus().getStatus().isComplete()) {
      throw new ISE("Query task [%s] should have finished", controllerTaskId);
    }

    return payload.getStatus().getErrorReport();

  }

  private void assertTalariaSpec(TalariaQuerySpec expectedTalariaQuerySpec, TalariaQuerySpec querySpecForTask)
  {

    Assert.assertEquals(
        expectedTalariaQuerySpec.getQuery().withOverriddenContext(querySpecForTask.getQuery().getContext()),
        querySpecForTask.getQuery()
    );
    Assert.assertEquals(
        expectedTalariaQuerySpec.getAssignmentStrategy(),
        querySpecForTask.getAssignmentStrategy()
    );
    Assert.assertEquals(
        expectedTalariaQuerySpec.getColumnMappings(),
        querySpecForTask.getColumnMappings()
    );
    Assert.assertEquals(
        expectedTalariaQuerySpec.getDestination(),
        querySpecForTask.getDestination()
    );
  }

  private void assertTuningConfig(
      ParallelIndexTuningConfig expectedTuningConfig,
      ParallelIndexTuningConfig tuningConfig
  )
  {
    Assert.assertEquals(
        expectedTuningConfig.getMaxNumConcurrentSubTasks(),
        tuningConfig.getMaxNumConcurrentSubTasks()
    );
    Assert.assertEquals(
        expectedTuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxRowsInMemory()
    );
    Assert.assertEquals(
        expectedTuningConfig.getPartitionsSpec(),
        tuningConfig.getPartitionsSpec()
    );
  }

  private Optional<Pair<RowSignature, List<Object[]>>> getSignatureWithRows(TalariaResultsReport resultsReport)
  {
    if (resultsReport == null) {
      return Optional.empty();
    } else {
      RowSignature rowSignature = resultsReport.getSignature();
      Yielder<Object[]> yielder = resultsReport.getResultYielder();
      List<Object[]> rows = new ArrayList<>();
      while (!yielder.isDone()) {
        rows.add(yielder.get());
        yielder = yielder.next(null);
      }
      try {
        yielder.close();
      }
      catch (IOException e) {
        throw new ISE("Unable to get results from the report");
      }

      return Optional.of(new Pair(rowSignature, rows));
    }
  }


  public abstract class TalariaQueryTester<Builder extends TalariaQueryTester>
  {
    protected String sql = null;
    protected Map<String, Object> queryContext = DEFAULT_TALARIA_CONTEXT;
    protected RowSignature expectedRowSignature = null;
    protected TalariaQuerySpec expectedTalariaQuerySpec = null;
    protected ParallelIndexTuningConfig expectedTuningConfig = null;
    protected Set<SegmentId> expectedSegments = null;
    protected List<Object[]> expectedResultRows = null;
    protected Matcher<Throwable> expectedValidationErrorMatcher = null;
    protected Matcher<Throwable> expectedExecutionErrorMatcher = null;
    protected TalariaFault expectedTalariaFault = null;

    private boolean hasRun = false;

    public Builder setSql(String sql)
    {
      this.sql = sql;
      return (Builder) this;
    }

    public Builder setQueryContext(Map<String, Object> queryContext)
    {
      this.queryContext = queryContext;
      return (Builder) this;
    }

    public Builder setExpectedRowSignature(RowSignature expectedRowSignature)
    {
      Preconditions.checkArgument(!expectedRowSignature.equals(RowSignature.empty()), "Row signature cannot be empty");
      this.expectedRowSignature = expectedRowSignature;
      return (Builder) this;
    }

    public Builder setExpectedSegment(Set<SegmentId> expectedSegments)
    {
      Preconditions.checkArgument(!expectedSegments.isEmpty(), "Segments cannot be empty");
      this.expectedSegments = expectedSegments;
      return (Builder) this;
    }

    public Builder setExpectedResultRows(List<Object[]> expectedResultRows)
    {
      Preconditions.checkArgument(expectedResultRows.size() > 0, "Results rows cannot be empty");
      this.expectedResultRows = expectedResultRows;
      return (Builder) this;
    }


    public Builder setExpectedTalariaQuerySpec(TalariaQuerySpec expectedTalariaQuerySpec)
    {
      this.expectedTalariaQuerySpec = expectedTalariaQuerySpec;
      return (Builder) this;
    }


    public Builder setExpectedValidationErrorMatcher(Matcher<Throwable> expectedValidationErrorMatcher)
    {
      this.expectedValidationErrorMatcher = expectedValidationErrorMatcher;
      return (Builder) this;
    }

    public Builder setExpectedExecutionErrorMatcher(Matcher<Throwable> expectedExecutionErrorMatcher)
    {
      this.expectedExecutionErrorMatcher = expectedExecutionErrorMatcher;
      return (Builder) this;
    }

    public Builder setExpectedTalariaFault(TalariaFault talariaFault)
    {
      this.expectedTalariaFault = talariaFault;
      return (Builder) this;
    }


    public void verifyPlanningErrors()
    {

      Preconditions.checkArgument(expectedValidationErrorMatcher != null, "Validation error matcher cannot be null");
      Preconditions.checkArgument(sql != null, "Sql cannot be null");
      readyToRun();
      try {

        if (planQueryOnly(sql, queryContext)) {
          throw new ISE("Query %s should have thrown an error.", sql);
        }
      }
      catch (Exception e) {
        MatcherAssert.assertThat(e, expectedValidationErrorMatcher);
      }
    }

    protected void readyToRun()
    {
      if (!hasRun) {
        hasRun = true;
      } else {
        throw new ISE("Use one @Test method per tester");
      }
    }
  }

  public class IngestQueryTester extends TalariaTestRunner.TalariaQueryTester<IngestQueryTester>
  {
    private String expectedDataSource;

    private Class<? extends ShardSpec> expectedShardSpec = NumberedShardSpec.class;

    private boolean expectedRollUp = false;

    private Granularity expectedQueryGranularity = Granularities.NONE;

    private List<AggregatorFactory> expectedAggregatorFactories = new ArrayList<>();

    private List<Interval> expectedDestinationIntervals = null;

    private IngestQueryTester()
    {
      // nothing to do
    }


    public IngestQueryTester setExpectedDataSource(String expectedDataSource)
    {
      this.expectedDataSource = expectedDataSource;
      return this;
    }

    public IngestQueryTester setExpectedShardSpec(Class<? extends ShardSpec> expectedShardSpec)
    {
      this.expectedShardSpec = expectedShardSpec;
      return this;
    }

    public IngestQueryTester setExpectedDestinationIntervals(List<Interval> expectedDestinationIntervals)
    {
      this.expectedDestinationIntervals = expectedDestinationIntervals;
      return this;
    }

    public IngestQueryTester setExpectedRollUp(boolean expectedRollUp)
    {
      this.expectedRollUp = expectedRollUp;
      return this;
    }

    public IngestQueryTester setExpectedQueryGranularity(Granularity expectedQueryGranularity)
    {
      this.expectedQueryGranularity = expectedQueryGranularity;
      return this;
    }

    public IngestQueryTester addExpectedAggregatorFactory(AggregatorFactory aggregatorFactory)
    {
      expectedAggregatorFactories.add(aggregatorFactory);
      return this;
    }

    public void verifyResults()
    {
      Preconditions.checkArgument(sql != null, "sql cannot be null");
      Preconditions.checkArgument(queryContext != null, "queryContext cannot be null");
      Preconditions.checkArgument(expectedDataSource != null, "dataSource cannot be null");
      Preconditions.checkArgument(expectedRowSignature != null, "expectedRowSignature cannot be null");
      Preconditions.checkArgument(
          expectedResultRows != null || expectedTalariaFault != null,
          "atleast one of expectedResultRows or expectedTalariaFault should be set to non null"
      );
      Preconditions.checkArgument(expectedShardSpec != null, "shardSpecClass cannot be null");
      readyToRun();
      try {
        String controllerId = runTalariaQuery(sql, queryContext);
        if (expectedTalariaFault != null) {
          MSQErrorReport msqErrorReport = getErrorReportOrThrow(controllerId);
          Assert.assertEquals(
              expectedTalariaFault.getCodeWithMessage(),
              msqErrorReport.getFault().getCodeWithMessage()
          );
          return;
        }
        getPayloadOrThrow(controllerId);
        TalariaQuerySpec foundSpec = indexingServiceClient.getQuerySpecForTask(controllerId);
        log.info(
            "found generated segments: %s",
            segmentManager.getAllDataSegments().stream().map(s -> s.toString()).collect(
                Collectors.joining("\n"))
        );
        //check if segments are created
        Assert.assertNotEquals(0, segmentManager.getAllDataSegments().size());


        String foundDataSource = null;
        SortedMap<SegmentId, List<List<Object>>> segmentIdVsOutputRowsMap = new TreeMap<>();
        for (DataSegment dataSegment : segmentManager.getAllDataSegments()) {

          //Assert shard spec class
          Assert.assertEquals(expectedShardSpec, dataSegment.getShardSpec().getClass());
          if (foundDataSource == null) {
            foundDataSource = dataSegment.getDataSource();

          } else if (!foundDataSource.equals(dataSegment.getDataSource())) {
            throw new ISE(
                "Expected only one datasource in the list of generated segments found [%s,%s]",
                foundDataSource,
                dataSegment.getDataSource()
            );
          }
          final QueryableIndex queryableIndex = indexIO.loadIndex(segmentCacheManager.getSegmentFiles(
              dataSegment));
          final StorageAdapter storageAdapter = new QueryableIndexStorageAdapter(queryableIndex);

          // assert rowSignature
          Assert.assertEquals(expectedRowSignature, storageAdapter.getRowSignature());

          // assert rollup
          Assert.assertEquals(expectedRollUp, queryableIndex.getMetadata().isRollup());

          // asset query granularity
          Assert.assertEquals(expectedQueryGranularity, queryableIndex.getMetadata().getQueryGranularity());

          // assert aggregator factories
          Assert.assertArrayEquals(
              expectedAggregatorFactories.toArray(new AggregatorFactory[0]),
              queryableIndex.getMetadata().getAggregators()
          );

          for (List<Object> row : FrameTestUtil.readRowsFromAdapter(storageAdapter, null, false).toList()) {
            // transforming rows for sketch assertions
            List<Object> transformedRow = row.stream()
                                             .map(r -> {
                                               if (r instanceof HyperLogLogCollector) {
                                                 return ((HyperLogLogCollector) r).estimateCardinalityRound();
                                               } else {
                                                 return r;
                                               }
                                             })
                                             .collect(Collectors.toList());
            segmentIdVsOutputRowsMap.computeIfAbsent(dataSegment.getId(), r -> new ArrayList<>()).add(transformedRow);
          }
        }

        log.info("Found spec: %s", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(foundSpec));
        List<Object[]> transformedOutputRows = segmentIdVsOutputRowsMap.values()
                                                                       .stream()
                                                                       .flatMap(Collection::stream)
                                                                       .map(List::toArray)
                                                                       .collect(Collectors.toList());

        log.info(
            "Found rows which are sorted forcefully %s",
            transformedOutputRows.stream().map(a -> Arrays.toString(a)).collect(Collectors.joining("\n"))
        );


        // assert data source name
        Assert.assertEquals(expectedDataSource, foundDataSource);
        // assert spec
        if (expectedTalariaQuerySpec != null) {
          assertTalariaSpec(expectedTalariaQuerySpec, foundSpec);
        }
        if (expectedTuningConfig != null) {
          assertTuningConfig(expectedTuningConfig, foundSpec.getTuningConfig());
        }
        if (expectedDestinationIntervals != null) {
          Assert.assertNotNull(foundSpec);
          DataSourceMSQDestination destination = (DataSourceMSQDestination) foundSpec.getDestination();
          Assert.assertEquals(expectedDestinationIntervals, destination.getReplaceTimeChunks());
        }
        if (expectedSegments != null) {
          Assert.assertEquals(expectedSegments, segmentIdVsOutputRowsMap.keySet());
          for (Object[] row : transformedOutputRows) {
            List<SegmentId> diskSegmentList = segmentIdVsOutputRowsMap.keySet()
                                                                      .stream()
                                                                      .filter(segmentId -> segmentId.getInterval()
                                                                                                    .contains((Long) row[0]))
                                                                      .collect(Collectors.toList());
            if (diskSegmentList.size() != 1) {
              throw new IllegalStateException("Single key in multiple partitions");
            }
            SegmentId diskSegment = diskSegmentList.get(0);
            // Checking if the row belongs to the correct segment interval
            Assert.assertTrue(segmentIdVsOutputRowsMap.get(diskSegment).contains(Arrays.asList(row)));
          }
        }
        // assert results
        assertResultsEquals(sql, expectedResultRows, transformedOutputRows);
      }
      catch (Exception e) {
        throw new ISE(e, "Query %s failed", sql);
      }
    }

    public void verifyExecutionError()
    {
      Preconditions.checkArgument(sql != null, "sql cannot be null");
      Preconditions.checkArgument(queryContext != null, "queryContext cannot be null");
      Preconditions.checkArgument(expectedExecutionErrorMatcher != null, "Execution error matcher cannot be null");
      readyToRun();
      try {
        String controllerId = runTalariaQuery(sql, queryContext);
        getPayloadOrThrow(controllerId);
        Assert.fail(StringUtils.format("Query did not throw an exception (sql = [%s])", sql));
      }
      catch (Exception e) {
        MatcherAssert.assertThat(
            StringUtils.format("Query error did not match expectations (sql = [%s])", sql),
            e,
            expectedExecutionErrorMatcher
        );
      }
    }
  }

  public class SelectQueryTester extends TalariaTestRunner.TalariaQueryTester<SelectQueryTester>
  {
    private SelectQueryTester()
    {
      // nothing to do
    }

    // Made the visibility public to aid adding ut's easily with minimum parameters to set.
    @Nullable
    public Pair<TalariaQuerySpec, Pair<RowSignature, List<Object[]>>> runQueryWithResult()
    {
      readyToRun();
      Preconditions.checkArgument(sql != null, "sql cannot be null");
      Preconditions.checkArgument(queryContext != null, "queryContext cannot be null");

      try {
        String controllerId = runTalariaQuery(sql, queryContext);

        if (expectedTalariaFault != null) {
          MSQErrorReport msqErrorReport = getErrorReportOrThrow(controllerId);
          Assert.assertEquals(
              expectedTalariaFault.getCodeWithMessage(),
              msqErrorReport.getFault().getCodeWithMessage()
          );
          return null;
        }

        TalariaTaskReportPayload payload = getPayloadOrThrow(controllerId);

        if (payload.getStatus().getErrorReport() != null) {
          throw new ISE("Query %s failed due to %s", sql, payload.getStatus().getErrorReport().toString());
        } else {
          Optional<Pair<RowSignature, List<Object[]>>> rowSignatureListPair = getSignatureWithRows(payload.getResults());
          if (!rowSignatureListPair.isPresent()) {
            throw new ISE("Query successful but no results found");
          }
          log.info("found row signature %s", rowSignatureListPair.get().lhs);
          log.info(rowSignatureListPair.get().rhs.stream()
                                                 .map(row -> Arrays.toString(row))
                                                 .collect(Collectors.joining("\n")));

          TalariaQuerySpec spec = indexingServiceClient.getQuerySpecForTask(controllerId);
          log.info("Found spec: %s", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(spec));
          return new Pair<>(spec, rowSignatureListPair.get());
        }
      }
      catch (Exception e) {
        if (expectedExecutionErrorMatcher == null) {
          throw new ISE(e, "Query %s failed", sql);
        }
        MatcherAssert.assertThat(e, expectedExecutionErrorMatcher);
        return null;
      }
    }

    public void verifyResults()
    {
      Preconditions.checkArgument(expectedResultRows != null, "Result rows cannot be null");
      Preconditions.checkArgument(expectedRowSignature != null, "Row signature cannot be null");
      Preconditions.checkArgument(expectedTalariaQuerySpec != null, "Talaria Query spec not ");
      Pair<TalariaQuerySpec, Pair<RowSignature, List<Object[]>>> specAndResults = runQueryWithResult();

      if (specAndResults == null) { // A fault was expected and the assertion has been done in the runQueryWithResult
        return;
      }

      Assert.assertEquals(expectedRowSignature, specAndResults.rhs.lhs);
      assertResultsEquals(sql, expectedResultRows, specAndResults.rhs.rhs);
      assertTalariaSpec(expectedTalariaQuerySpec, specAndResults.lhs);
    }

    public void verifyExecutionError()
    {
      Preconditions.checkArgument(expectedExecutionErrorMatcher != null, "Execution error matcher cannot be null");
      if (runQueryWithResult() != null) {
        throw new ISE("Query %s did not throw an exception", sql);
      }
    }
  }
}
