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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class IpAddressTopNQueryTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final QueryContexts.Vectorize vectorize;
  private final AggregationTestHelper helper;
  private final List<Segment> segments;
  private final boolean useRealtimeSegments;

  public IpAddressTopNQueryTest(String vectorize, boolean useRealtimeSegments) throws Exception
  {
    NullHandling.initializeForTests();
    IpAddressModule.registerHandlersAndSerde();
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.helper = AggregationTestHelper.createTopNQueryAggregationTestHelper(
        IpAddressTestUtils.LICENSED_IP_ADDRESS_MODULE.getJacksonModules(),
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

  @Parameterized.Parameters(name = "vectorize = {0}, useRealtimeSegments = {1}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (String vectorize : new String[]{"false", "true", "force"}) {
      for (boolean useRealtimeSegments : new boolean[]{true, false}) {
        constructors.add(new Object[]{vectorize, useRealtimeSegments});
      }
    }
    return constructors;
  }

  @Test
  public void testTopNStringifyVirtualColumnWithoutNull()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("test_datasource")
        .granularity(Granularities.ALL)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimension(DefaultDimensionSpec.of("v0"))
        .virtualColumns(
            new IpAddressFormatVirtualColumn(
                "v0",
                "ipv4",
                true,
                false
            )
        )
        .metric("count")
        .filters(BaseCalciteQueryTest.not(BaseCalciteQueryTest.selector("v0", null, null)))
        .threshold(2)
        .aggregators(new CountAggregatorFactory("count"))
        .context(getContext())
        .build();
    List rows = helper.runQueryOnSegmentsObjs(segments, query).toList();
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2014-10-20T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "1.2.3.4"
                    ),
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "10.10.10.11"
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, rows);
  }

  @Test
  public void testTopNStringifyVirtualColumnWithNull()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("test_datasource")
        .granularity(Granularities.ALL)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimension(DefaultDimensionSpec.of("v0"))
        .virtualColumns(
            new IpAddressFormatVirtualColumn(
                "v0",
                "ipv4",
                true,
                false
            )
        )
        .metric("count")
        .threshold(3)
        .aggregators(new CountAggregatorFactory("count"))
        .context(getContext())
        .build();

    List rows = helper.runQueryOnSegmentsObjs(segments, query).toList();
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2014-10-20T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new HashMap<String, Object>() {{
                      put("count", 2);
                      put("v0", null);
                    }},
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "1.2.3.4"
                    ),
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "10.10.10.11"
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, rows);
  }

  @Test
  public void testTopNStringifyVirtualColumnSelectorFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("test_datasource")
        .granularity(Granularities.ALL)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimension(DefaultDimensionSpec.of("v0"))
        .virtualColumns(
            new IpAddressFormatVirtualColumn(
                "v0",
                "ipv4",
                true,
                false
            )
        )
        .filters(
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
        .metric("count")
        .threshold(2)
        .aggregators(new CountAggregatorFactory("count"))
        .context(getContext())
        .build();

    List rows = helper.runQueryOnSegmentsObjs(segments, query).toList();
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2014-10-20T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "1.2.3.4"
                    ),
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "100.200.123.12"
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, rows);
  }

  @Test
  public void testTopNStringifyVirtualColumnBoundFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("test_datasource")
        .granularity(Granularities.ALL)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimension(DefaultDimensionSpec.of("v0"))
        .virtualColumns(
            new IpAddressFormatVirtualColumn(
                "v0",
                "ipv4",
                true,
                false
            )
        )
        .filters(
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
        .metric("count")
        .threshold(2)
        .aggregators(new CountAggregatorFactory("count"))
        .context(getContext())
        .build();

    List rows = helper.runQueryOnSegmentsObjs(segments, query).toList();
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2014-10-20T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "10.10.10.11"
                    ),
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "22.22.23.24"
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, rows);
  }

  @Test
  public void testTopNStringifyVirtualColumnForcev6Compact()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("test_datasource")
        .granularity(Granularities.ALL)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimension(DefaultDimensionSpec.of("v0"))
        .virtualColumns(
            new IpAddressFormatVirtualColumn(
                "v0",
                "ipv4",
                true,
                true
            )
        )
        .metric("count")
        .threshold(3)
        .aggregators(new CountAggregatorFactory("count"))
        .context(getContext())
        .build();

    List rows = helper.runQueryOnSegmentsObjs(segments, query).toList();
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2014-10-20T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new HashMap<String, Object>() {{
                      put("count", 2);
                      put("v0", null);
                    }},
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "::ffff:102:304"
                    ),
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "::ffff:1616:1718"
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, rows);
  }

  @Test
  public void testTopNStringifyWithoutNull()
  {
    ExpressionProcessing.initializeForTests(null);
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("test_datasource")
        .granularity(Granularities.ALL)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimension(DefaultDimensionSpec.of("v0"))
        .virtualColumns(
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
        .metric("count")
        .filters(BaseCalciteQueryTest.not(BaseCalciteQueryTest.selector("v0", null, null)))
        .threshold(2)
        .aggregators(new CountAggregatorFactory("count"))
        .context(getContext())
        .build();
    List rows = helper.runQueryOnSegmentsObjs(segments, query).toList();
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2014-10-20T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "1.2.3.4"
                    ),
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "10.10.10.11"
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, rows);
  }


  @Test
  public void testTopNStringifyWithNull()
  {
    ExpressionProcessing.initializeForTests(null);
    TopNQuery query = new TopNQueryBuilder()
        .dataSource("test_datasource")
        .granularity(Granularities.ALL)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimension(DefaultDimensionSpec.of("v0"))
        .virtualColumns(
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
        .metric("count")
        .threshold(3)
        .aggregators(new CountAggregatorFactory("count"))
        .context(getContext())
        .build();

    List rows = helper.runQueryOnSegmentsObjs(segments, query).toList();
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2014-10-20T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new HashMap<String, Object>() {{
                      put("count", 2);
                      put("v0", null);
                    }},
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "1.2.3.4"
                    ),
                    ImmutableMap.of(
                        "count", 2,
                        "v0", "10.10.10.11"
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, rows);
  }
}
