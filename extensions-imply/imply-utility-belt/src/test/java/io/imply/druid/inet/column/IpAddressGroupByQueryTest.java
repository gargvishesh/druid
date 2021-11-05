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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
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

  public IpAddressGroupByQueryTest(GroupByQueryConfig config, String vectorize)
  {
    IpAddressModule.registerHandlersAndSerde();
    this.config = config;
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new IpAddressModule().getJacksonModules(),
        config,
        tempFolder
    );
  }

  @Parameterized.Parameters(name = "config = {0}, vectorize = {1}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (String vectorize : new String[]{"false", "true", "force"}) {
        constructors.add(new Object[]{config, vectorize});
      }
    }
    return constructors;
  }

  @Test
  public void testGroupBy() throws Exception
  {
    // this is pretty wack, but just documenting the cuurrent behavior
    GroupByQuery groupQuery = GroupByQuery.builder()
                                              .setDataSource("test_datasource")
                                              .setGranularity(Granularities.ALL)
                                              .setInterval(Intervals.ETERNITY)
                                              .setDimensions(DefaultDimensionSpec.of("ipv4"))
                                              .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                              .setContext(ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize.toString()))
                                              .build();

    List<Segment> segs = IpAddressTestUtils.createDefaultHourlySegments(helper, tempFolder);

    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segs, groupQuery);

    List<ResultRow> results = seq.toList();

    if (GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy()) ||
        vectorize == QueryContexts.Vectorize.FALSE) {
      // since we happen to implement a string dimension selector so that we can re-use dictionary encoded column
      // indexing, group by v1 and v2 work because of the "we'll do it live! fuck it!" principle
      verifyResults(
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
      verifyResults(
          groupQuery.getResultRowSignature(),
          results,
          ImmutableList.of(
              new Object[]{null, 10L}
          )
      );
    }
  }

  @Test
  public void testGroupByTypedDimSpec() throws Exception
  {
    // if the correct type is used, then everything fails as expected
    if (GroupByStrategySelector.STRATEGY_V1.equals(config.getDefaultStrategy())) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage(
          "GroupBy v1 only supports dimensions with an outputType of STRING."
      );
    }
    else {
      expectedException.expect(IAE.class);
      expectedException.expectMessage("invalid type: COMPLEX<ipAddress>");
    }
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(new DefaultDimensionSpec("ipv4", "ipv4", IpAddressModule.TYPE))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize.toString()))
                                          .build();

    List<Segment> segs = IpAddressTestUtils.createDefaultHourlySegments(helper, tempFolder);

    helper.runQueryOnSegmentsObjs(segs, groupQuery).toList();

    Assert.fail();
  }

  private static void verifyResults(RowSignature rowSignature, List<ResultRow> results, List<Object[]> expected)
  {
    Assert.assertEquals(expected.size(), results.size());
    for (int i = 0; i < expected.size(); i++) {
      final Object[] resultRow = results.get(i).getArray();
      Assert.assertEquals(expected.get(i).length, resultRow.length);
      for (int j = 0; j < resultRow.length; j++) {
        if (rowSignature.getColumnType(j).map(t -> t.anyOf(ValueType.DOUBLE, ValueType.FLOAT)).orElse(false)) {
          Assert.assertEquals((Double) resultRow[j], (Double) expected.get(i)[j], 0.01);
        } else {
          Assert.assertEquals(resultRow[j], expected.get(i)[j]);
        }
      }
    }
  }
}
