/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.util.Modules;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQueryModule;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQueryRunnerFactory;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQueryToolChest;
import io.imply.druid.query.samplinggroupby.metrics.DefaultSamplingGroupByQueryMetricsFactory;
import io.imply.druid.query.samplinggroupby.sql.calcite.rule.DruidSamplingGroupByQueryRule;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.TestBufferPool;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.rule.ExtensionCalciteRuleProvider;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework.Builder;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class SamplingGroupBySqlQueryTest extends BaseCalciteQueryTest
{
  @Override
  public QueryRunnerFactoryConglomerate createCongolmerate(
      Builder builder,
      Closer resourceCloser
  )
  {
    QueryRunnerFactoryConglomerate conglomerate = super.createCongolmerate(builder, resourceCloser);
    return new DelegatingQueryRunnerFactoryConglomerate(conglomerate, resourceCloser);
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    // This is a hack to override a druid module such that only the QueryRunnerFactory bindings are overriden.
    // The earlier interface used to provide separate hooks for expressions and sql bindings which worked fine.
    // The new interface requires to bind a single module with both, but incase of SamplingGroupBy the expressions,
    // sql bindings and query runner factory are attached to the same module. So, adding that module fails since the
    // QueryRunnerFactory doesn't find required bindings.
    // We supply testing QueryRunnerFactory for SamplingGroupBy via createCongolmerate method.
    SamplingGroupByQueryModule originalModule = new SamplingGroupByQueryModule();
    com.google.inject.Module overriddenModule = Modules.override(originalModule).with(
        binder -> binder.bind(SamplingGroupByQueryRunnerFactory.class)
              .toInstance(new SamplingGroupByQueryRunnerFactory(
                  new SamplingGroupByQueryToolChest(DefaultSamplingGroupByQueryMetricsFactory.instance()),
                  new DruidProcessingConfig()
                  {
                    @Override
                    public String getFormatString()
                    {
                      return null;
                    }
                  },
                  null,
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              ))
    );
    builder.addModule(
        new DruidModule()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.install(overriddenModule);
          }

          @Override
          public List<? extends Module> getJacksonModules()
          {
            return originalModule.getJacksonModules();
          }
        }
    );
  }

  @Override
  public Set<ExtensionCalciteRuleProvider> extensionCalciteRules()
  {
    return ImmutableSet.of(new DruidSamplingGroupByQueryRule.DruidSamplingGroupByQueryRuleProvider());
  }

  private static class DelegatingQueryRunnerFactoryConglomerate implements QueryRunnerFactoryConglomerate
  {
    private final QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate;
    private final QueryRunnerFactory<ResultRow, SamplingGroupByQuery> samplingQueryRunnerFactory;

    public DelegatingQueryRunnerFactoryConglomerate(
        QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate,
        Closer closer)
    {
      this.queryRunnerFactoryConglomerate = queryRunnerFactoryConglomerate;

      final TestBufferPool testBufferPool = TestBufferPool.offHeap(10 * 1024 * 1024, Integer.MAX_VALUE);
      closer.register(() -> {
        // Verify that all objects have been returned to the pool.
        Assert.assertEquals(0, testBufferPool.getOutstandingObjectCount());
      });
      this.samplingQueryRunnerFactory = new SamplingGroupByQueryRunnerFactory(
          new SamplingGroupByQueryToolChest(DefaultSamplingGroupByQueryMetricsFactory.instance()),
          new DruidProcessingConfig()
          {
            @Override
            public String getFormatString()
            {
              return null;
            }
          },
          testBufferPool,
          QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );
    }

    @Override
    public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(
        QueryType query
    )
    {
      if (query instanceof SamplingGroupByQuery) {
        return (QueryRunnerFactory<T, QueryType>) samplingQueryRunnerFactory;
      }
      return queryRunnerFactoryConglomerate.findFactory(query);
    }
  }

  @Test
  public void testFullSamplingGroupByQuery()
  {
    testQuery(
        "SELECT * from (SELECT dim1, sum(m1), SAMPLING_RATE() as s from foo GROUP BY dim1) TABLESAMPLE SYSTEM(10 rows)",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(new QueryDataSource(
                      SamplingGroupByQuery.builder()
                                          .setDataSource("foo")
                                          .setInterval(querySegmentSpec(Intervals.ETERNITY))
                                          .setDimensions(new DefaultDimensionSpec("dim1", "d0"))
                                          .setAggregatorSpecs(ImmutableList.of(
                                              new DoubleSumAggregatorFactory("a0", "m1")
                                          ))
                                          .setPostAggregatorSpecs(ImmutableList.of(
                                              new FieldAccessPostAggregator(
                                                "a1",
                                                SamplingGroupByQuery.SAMPLING_RATE_DIMESION_NAME
                                            )
                                          ))
                                        .setGranularity(Granularities.ALL)
                                        .setMaxGroups(10)
                                        .build()
                              )
                  )
                  .eternityInterval()
                  .columns("a0", "a1", "d0")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 2.0D, 1.0D},
            new Object[]{"2", 3.0D, 1.0D},
            new Object[]{"def", 5.0D, 1.0D},
            new Object[]{"1", 4.0D, 1.0D},
            new Object[]{"abc", 6.0D, 1.0D},
            new Object[]{"", 1.0D, 1.0D}
        )
    );
  }

  @Test
  public void testPartialSamplingGroupByQuery()
  {
    testQuery(
        "SELECT * from (SELECT dim1, sum(m1), SAMPLING_RATE() as s from foo GROUP BY dim1) TABLESAMPLE SYSTEM(2 rows)",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(new QueryDataSource(
                      SamplingGroupByQuery.builder()
                                          .setDataSource("foo")
                                          .setInterval(querySegmentSpec(Intervals.ETERNITY))
                                          .setDimensions(new DefaultDimensionSpec("dim1", "d0"))
                                          .setAggregatorSpecs(ImmutableList.of(
                                              new DoubleSumAggregatorFactory("a0", "m1")
                                          ))
                                          .setPostAggregatorSpecs(ImmutableList.of(
                                              new FieldAccessPostAggregator(
                                                  "a1",
                                                  SamplingGroupByQuery.SAMPLING_RATE_DIMESION_NAME
                                              )
                                          ))
                                          .setGranularity(Granularities.ALL)
                                          .setMaxGroups(2)
                                          .build()
                              )
                  )
                  .eternityInterval()
                  .columns("a0", "a1", "d0")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 2.0D, 0.3023570458822361D},
            new Object[]{"2", 3.0D, 0.3023570458822361D}
        )
    );
  }

  @Test
  public void testExtrapolationSamplingGroupByQuery()
  {
    // don't check this test for correctness of results as compared to exact query
    // this is to test the functionality of sampling
    cannotVectorize();
    testQuery(
        "SELECT sum(s) / r, r from (SELECT sum(m1) as s, SAMPLING_RATE() as r from foo GROUP BY dim1) "
        + "TABLESAMPLE SYSTEM(2 rows) GROUP BY r",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(new QueryDataSource(
                            SamplingGroupByQuery.builder()
                                                .setDataSource("foo")
                                                .setInterval(querySegmentSpec(Intervals.ETERNITY))
                                                .setDimensions(
                                                    new DefaultDimensionSpec("dim1", "d0")
                                                )
                                                .setAggregatorSpecs(ImmutableList.of(
                                                    new DoubleSumAggregatorFactory("a0", "m1")
                                                ))
                                                .setPostAggregatorSpecs(ImmutableList.of(
                                                    new FieldAccessPostAggregator(
                                                        "a1",
                                                        SamplingGroupByQuery.SAMPLING_RATE_DIMESION_NAME
                                                    )
                                                ))
                                                .setGranularity(Granularities.ALL)
                                                .setMaxGroups(2)
                                                .build()
                              )
                        )
                        .setDimensions(new DefaultDimensionSpec("a1", "d0", ColumnType.DOUBLE))
                        .setAggregatorSpecs(ImmutableList.of(
                            new DoubleSumAggregatorFactory("_a0", "a0")
                        ))
                        .setPostAggregatorSpecs(
                            expressionPostAgg(
                                "p0",
                                "(\"_a0\" / \"d0\")",
                                ColumnType.DOUBLE
                            )
                        )
                        .setInterval(querySegmentSpec(Intervals.ETERNITY))
                        .setGranularity(Granularities.ALL)
                        .build()
        ),
        ImmutableList.of(
            // the rows returned by sampling contains the measure values as [2, 3] (as tested above)
            // with 0.3 as the sampling rate, the total sum is expected to be ~ 16
            new Object[]{16.536740479821432D, 0.3023570458822361D}
        )
    );
  }

  @Test
  public void testSessionizeJoinUsingSamplingGroupByQuery()
  {
    testQuery(
        "SELECT foo.m1, sample.dim1 from foo join ( " +
        "SELECT * from (SELECT dim1, SAMPLING_RATE() as r from foo GROUP BY 1) "
        + "TABLESAMPLE SYSTEM(2 rows) ) sample ON foo.dim1 = sample.dim1",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      JoinDataSource.create(
                          new TableDataSource("foo"),
                          new QueryDataSource(
                              Druids.newScanQueryBuilder()
                                    .dataSource(
                                        new QueryDataSource(
                                            SamplingGroupByQuery
                                                .builder()
                                                .setDataSource("foo")
                                                .setInterval(querySegmentSpec(Intervals.ETERNITY))
                                                .setDimensions(
                                                    new DefaultDimensionSpec("dim1", "d0")
                                                )
                                                .setAggregatorSpecs(ImmutableList.of())
                                                .setPostAggregatorSpecs(ImmutableList.of(
                                                    new FieldAccessPostAggregator(
                                                        "a0",
                                                        SamplingGroupByQuery.SAMPLING_RATE_DIMESION_NAME
                                                    )
                                                ))
                                                .setGranularity(Granularities.ALL)
                                                .setMaxGroups(2)
                                                .build()
                                        )
                                    )
                                    .columns("d0")
                                    .eternityInterval()
                                    .legacy(false)
                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                    .build()
                          ),
                          "j0.",
                          "(\"dim1\" == \"j0.d0\")",
                          JoinType.INNER,
                          null,
                          ExprMacroTable.nil(),
                          CalciteTests.createJoinableFactoryWrapper()
                      )
                  )
                  .columns("j0.d0", "m1")
                  .eternityInterval()
                  .legacy(false)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2.0F, "10.1"},
            new Object[]{3.0F, "2"}
        )
    );
  }

  @Test
  public void testSessionizeAndExtrapolatingJoinUsingSamplingGroupByQuery()
  {
    // don't check this test for correctnes of results as comapred to exact query
    // this is to test the functionality of sampling
    cannotVectorize();
    testQuery(
        "SELECT sum(foo.m1) / r, r from foo join ( " +
        "SELECT * from (SELECT dim1, SAMPLING_RATE() as r from foo GROUP BY 1) "
        + "TABLESAMPLE SYSTEM(2 rows) ) sample ON foo.dim1 = sample.dim1 GROUP BY r",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            JoinDataSource.create(
                                new TableDataSource("foo"),
                                new QueryDataSource(
                                    Druids.newScanQueryBuilder()
                                          .dataSource(
                                              new QueryDataSource(
                                                  SamplingGroupByQuery
                                                      .builder()
                                                      .setDataSource("foo")
                                                      .setInterval(querySegmentSpec(Intervals.ETERNITY))
                                                      .setDimensions(
                                                          new DefaultDimensionSpec("dim1", "d0")
                                                      )
                                                      .setAggregatorSpecs(ImmutableList.of())
                                                      .setPostAggregatorSpecs(ImmutableList.of(
                                                          new FieldAccessPostAggregator(
                                                              "a0",
                                                              SamplingGroupByQuery.SAMPLING_RATE_DIMESION_NAME
                                                          )
                                                      ))
                                                      .setGranularity(Granularities.ALL)
                                                      .setMaxGroups(2)
                                                      .build()
                                              )
                                          )
                                          .columns("a0", "d0")
                                          .eternityInterval()
                                          .legacy(false)
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                          .build()
                                ),
                                "j0.",
                                "(\"dim1\" == \"j0.d0\")",
                                JoinType.INNER,
                                null,
                                ExprMacroTable.nil(),
                                CalciteTests.createJoinableFactoryWrapper()
                            )
                        )
                        .setDimensions(new DefaultDimensionSpec("j0.a0", "d0", ColumnType.DOUBLE))
                        .setAggregatorSpecs(ImmutableList.of(
                            new DoubleSumAggregatorFactory("a0", "m1")
                        ))
                        .setPostAggregatorSpecs(
                            expressionPostAgg(
                                "p0",
                                "(\"a0\" / \"d0\")",
                                ColumnType.DOUBLE
                            )
                        )
                        .setInterval(querySegmentSpec(Intervals.ETERNITY))
                        .setGranularity(Granularities.ALL)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{16.536740479821432D, 0.3023570458822361D}
        )
    );
  }

  @Test
  public void testSessionizeJoinToFilterUsingSamplingGroupByQuery()
  {
    ImmutableMap.Builder<String, Object> queryContextBuilder = ImmutableMap.builder();
    queryContextBuilder.putAll(QUERY_CONTEXT_DEFAULT);
    queryContextBuilder.put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, true);

    testQuery(
        "SELECT foo.m1, sample.dim1, sample.r from foo join ( " +
        "SELECT * from (SELECT dim1, SAMPLING_RATE() as r from foo GROUP BY 1) "
        + "TABLESAMPLE SYSTEM(2 rows) ) sample ON foo.dim1 = sample.dim1",
        queryContextBuilder.build(),
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      JoinDataSource.create(
                          new TableDataSource("foo"),
                          new QueryDataSource(
                              Druids.newScanQueryBuilder()
                                    .dataSource(
                                        new QueryDataSource(
                                            SamplingGroupByQuery
                                                .builder()
                                                .setDataSource("foo")
                                                .setInterval(querySegmentSpec(Intervals.ETERNITY))
                                                .setDimensions(
                                                    new DefaultDimensionSpec("dim1", "d0")
                                                )
                                                .setAggregatorSpecs(ImmutableList.of())
                                                .setPostAggregatorSpecs(ImmutableList.of(
                                                    new FieldAccessPostAggregator(
                                                        "a0",
                                                        SamplingGroupByQuery.SAMPLING_RATE_DIMESION_NAME
                                                    )
                                                ))
                                                .setGranularity(Granularities.ALL)
                                                .setMaxGroups(2)
                                                .build()
                                        )
                                    )
                                    .columns("a0", "d0")
                                    .eternityInterval()
                                    .legacy(false)
                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                    .build()
                          ),
                          "j0.",
                          "(\"dim1\" == \"j0.d0\")",
                          JoinType.INNER,
                          null,
                          ExprMacroTable.nil(),
                          CalciteTests.createJoinableFactoryWrapper()
                      )
                  )
                  .columns("j0.a0", "j0.d0", "m1")
                  .eternityInterval()
                  .legacy(false)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2.0F, "10.1", 0.3023570458822361D},
            new Object[]{3.0F, "2", 0.3023570458822361D}
        )
    );
    // there is basically no way to write this test as it was previously written now
    // since segmentMapFn is defined on JoinDataSource, which is created during sql planning
    // and we have no way to override to create a 'capturing' version like the previous
    // test was doing with the JoinableFactoryWrapper.
    /*
    SegmentReference segmentReference = joinableFactoryWrapper.getInterceptedSegment();
    assert segmentReference instanceof HashJoinSegment;
    HashJoinSegment hashJoinSegment = (HashJoinSegment) segmentReference;
    Assert.assertEquals(
        hashJoinSegment.getBaseFilter(),
        new InDimFilter("dim1", ImmutableSet.of("10.1", "2"))
    );
    // the returned clause list is not comparable with an expected clause list since the Joinable
    // class member in JoinableClause doesn't implement equals method in its implementations
    Assert.assertEquals(hashJoinSegment.getClauses().size(), 1);
   */
  }
}
