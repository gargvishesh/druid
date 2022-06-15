/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.inet.IpAddressModule;
import io.imply.druid.inet.expression.IpAddressExpressions;
import io.imply.druid.inet.segment.virtual.IpAddressFormatVirtualColumn;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class IpAddressGroupByQueryTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final GroupByQueryConfig config;
  private final QueryContexts.Vectorize vectorize;
  private final AggregationTestHelper helper;
  private final List<Segment> segments;
  private final boolean useRealtimeSegments;

  public IpAddressGroupByQueryTest(GroupByQueryConfig config, String vectorize, boolean useRealtimeSegments) throws Exception
  {
    IpAddressModule.registerHandlersAndSerde();
    this.config = config;
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        IpAddressTestUtils.LICENSED_IP_ADDRESS_MODULE.getJacksonModules(),
        config,
        tempFolder
    );
    this.useRealtimeSegments = useRealtimeSegments;
    if (useRealtimeSegments) {
      this.segments = ImmutableList.of(IpAddressTestUtils.createIpAddressDefaultHourlyIncrementalIndex());
    } else {
      tempFolder.create();
      this.segments = IpAddressTestUtils.createIpAddressDefaultHourlySegments(helper, tempFolder);
    }
  }

  public Map<String, Object> getContext()
  {
    return ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize.toString(),
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, "true"
    );
  }

  @Parameterized.Parameters(name = "config = {0}, vectorize = {1}, useRealtimeSegments = {2}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (String vectorize : new String[]{"false", "true", "force"}) {
        for (boolean useRealtimeSegments : new boolean[]{true, false}) {
          constructors.add(new Object[]{config, vectorize, useRealtimeSegments});
        }
      }
    }
    return constructors;
  }

  @Test
  public void testGroupBy()
  {
    if (vectorize == QueryContexts.Vectorize.FORCE && useRealtimeSegments && !GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "Cannot vectorize!"
      );
    }

    // this is pretty wack, but just documenting the cuurrent behavior
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("ipv4"))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segments, groupQuery);

    List<ResultRow> results = seq.toList();

    if (GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy()) ||
        vectorize == QueryContexts.Vectorize.FALSE ||
        useRealtimeSegments
    ) {
      // since we happen to implement a string dimension selector so that we can re-use dictionary encoded column
      // indexing, group by v1 and v2 work because of the "we'll do it live! fuck it!" principle
      IpAddressTestUtils.verifyResults(
          groupQuery.getResultRowSignature(),
          results,
          ImmutableList.of(
              new Object[]{null, 1L},
              new Object[]{"1.2.3.4", 2L},
              new Object[]{"10.10.10.11", 2L},
              new Object[]{"100.200.123.12", 2L},
              new Object[]{"22.22.23.24", 2L},
              new Object[]{"5.6.7.8", 1L}
          )
      );
    } else {
      // the vector engine behaves according to the underlying types, so it does the "expected" thing and groups on null
      IpAddressTestUtils.verifyResults(
          groupQuery.getResultRowSignature(),
          results,
          ImmutableList.of(
              new Object[]{null, 10L}
          )
      );
    }
  }

  @Test
  public void testGroupByStringify()
  {
    if (GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "GroupBy v1 does not support dimension selectors with unknown cardinality."
      );
    } else if (vectorize == QueryContexts.Vectorize.FORCE) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "Cannot vectorize!"
      );
    }

    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(
                                              new ExpressionVirtualColumn(
                                                  "v0",
                                                  "ip_stringify(\"ipv4\")",
                                                  null,
                                                  new ExprMacroTable(
                                                      ImmutableList.of(
                                                          new IpAddressExpressions.StringifyExprMacro()
                                                      )
                                                  )
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segments, groupQuery);

    List<ResultRow> results = seq.toList();

    IpAddressTestUtils.verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(
            new Object[]{null, 1L},
            new Object[]{"1.2.3.4", 2L},
            new Object[]{"10.10.10.11", 2L},
            new Object[]{"100.200.123.12", 2L},
            new Object[]{"22.22.23.24", 2L},
            new Object[]{"5.6.7.8", 1L}
        )
    );
  }

  @Test
  public void testGroupByTypedDimSpec()
  {
    // if the correct type is used, then everything fails as expected
    if (GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "GroupBy v1 only supports dimensions with an outputType of STRING."
      );
    } else {
      expectedException.expect(IAE.class);
      expectedException.expectMessage("invalid type: COMPLEX<ipAddress>");
    }
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(new DefaultDimensionSpec("ipv4", "ipv4", IpAddressModule.ADDRESS_TYPE))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    helper.runQueryOnSegmentsObjs(segments, groupQuery).toList();

    Assert.fail();
  }

  @Test
  public void testGroupByStringifyVirtualColumn()
  {
    if (vectorize == QueryContexts.Vectorize.FORCE && useRealtimeSegments && !GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "Cannot vectorize!"
      );
    }

    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(
                                              new IpAddressFormatVirtualColumn(
                                                  "v0",
                                                  "ipv4",
                                                  true,
                                                  false
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segments, groupQuery);

    List<ResultRow> results = seq.toList();

    IpAddressTestUtils.verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(
            new Object[]{null, 1L},
            new Object[]{"1.2.3.4", 2L},
            new Object[]{"10.10.10.11", 2L},
            new Object[]{"100.200.123.12", 2L},
            new Object[]{"22.22.23.24", 2L},
            new Object[]{"5.6.7.8", 1L}
        )
    );
  }

  @Test
  public void testGroupByStringifyVirtualColumnSelectorFilter()
  {
    if (vectorize == QueryContexts.Vectorize.FORCE && useRealtimeSegments && !GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "Cannot vectorize!"
      );
    }
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(
                                              new IpAddressFormatVirtualColumn(
                                                  "v0",
                                                  "ipv4",
                                                  true,
                                                  false
                                              )
                                          )
                                          .setDimFilter(
                                              new OrDimFilter(
                                                  new SelectorDimFilter(
                                                      "v0",
                                                      "1.2.3.4",
                                                      null
                                                  ),
                                                  new SelectorDimFilter(
                                                      "v0",
                                                      "100.200.123.12",
                                                      null
                                                  )
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segments, groupQuery);

    List<ResultRow> results = seq.toList();

    IpAddressTestUtils.verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(
            new Object[]{"1.2.3.4", 2L},
            new Object[]{"100.200.123.12", 2L}
        )
    );
  }

  @Test
  public void testGroupByStringifyVirtualColumnBoundFilter()
  {
    if (vectorize == QueryContexts.Vectorize.FORCE && useRealtimeSegments && !GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "Cannot vectorize!"
      );
    }

    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(
                                              new IpAddressFormatVirtualColumn(
                                                  "v0",
                                                  "ipv4",
                                                  true,
                                                  false
                                              )
                                          )
                                          .setDimFilter(
                                              new BoundDimFilter(
                                                  "v0",
                                                  "10.10.10.10",
                                                  "25.35.45.55",
                                                  true,
                                                  true,
                                                  true,
                                                  null,
                                                  null
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segments, groupQuery);

    List<ResultRow> results = seq.toList();

    IpAddressTestUtils.verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(
            new Object[]{"10.10.10.11", 2L},
            new Object[]{"22.22.23.24", 2L}
        )
    );
  }

  @Test
  public void testGroupByStringifyVirtualColumnForcev6Compact()
  {
    if (vectorize == QueryContexts.Vectorize.FORCE && useRealtimeSegments && !GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "Cannot vectorize!"
      );
    }

    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(
                                              new IpAddressFormatVirtualColumn(
                                                  "v0",
                                                  "ipv4",
                                                  true,
                                                  true
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segments, groupQuery);

    List<ResultRow> results = seq.toList();

    IpAddressTestUtils.verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(
            new Object[]{null, 1L},
            new Object[]{"::ffff:102:304", 2L},
            new Object[]{"::ffff:1616:1718", 2L},
            new Object[]{"::ffff:506:708", 1L},
            new Object[]{"::ffff:64c8:7b0c", 2L},
            new Object[]{"::ffff:a0a:a0b", 2L}
        )
    );
  }
}
