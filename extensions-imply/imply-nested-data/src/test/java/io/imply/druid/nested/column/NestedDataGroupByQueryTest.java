/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.nested.NestedDataModule;
import io.imply.druid.nested.virtual.NestedFieldVirtualColumn;
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
import java.util.Map;
import java.util.function.BiFunction;

@RunWith(Parameterized.class)
public class NestedDataGroupByQueryTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final GroupByQueryConfig config;
  private final QueryContexts.Vectorize vectorize;
  private final AggregationTestHelper helper;
  private final BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>> segmentsGenerator;
  private final String segmentsName;

  public NestedDataGroupByQueryTest(
      GroupByQueryConfig config,
      BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>> segmentGenerator,
      String vectorize
  )
  {
    NestedDataModule.registerHandlersAndSerde();
    this.config = config;
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        NestedDataModule.getJacksonModulesList(),
        config,
        tempFolder
    );
    this.segmentsGenerator = segmentGenerator;
    this.segmentsName = segmentGenerator.toString();
  }

  public Map<String, Object> getContext()
  {
    return ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize.toString(),
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, "true"
    );
  }

  @Parameterized.Parameters(name = "config = {0}, segments = {1}, vectorize = {2}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    final List<BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>>> segmentsGenerators = new ArrayList<>();
    segmentsGenerators.add(new BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>>()
    {
      @Override
      public List<Segment> apply(AggregationTestHelper helper, TemporaryFolder tempFolder)
      {
        try {
          return ImmutableList.<Segment>builder()
                              .addAll(NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder))
                              .add(NestedDataTestUtils.createDefaultHourlyIncrementalIndex())
                              .build();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return "mixed";
      }
    });
    segmentsGenerators.add(new BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>>()
    {
      @Override
      public List<Segment> apply(AggregationTestHelper helper, TemporaryFolder tempFolder)
      {
        try {
          return ImmutableList.of(
              NestedDataTestUtils.createDefaultHourlyIncrementalIndex(),
              NestedDataTestUtils.createDefaultHourlyIncrementalIndex()
          );
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return "incremental";
      }
    });
    segmentsGenerators.add(new BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>>()
    {
      @Override
      public List<Segment> apply(AggregationTestHelper helper, TemporaryFolder tempFolder)
      {
        try {
          return ImmutableList.<Segment>builder()
                              .addAll(NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder))
                              .addAll(NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder))
                              .build();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return "segments";
      }
    });

    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>> generatorFn : segmentsGenerators) {
        for (String vectorize : new String[]{"false", "true", "force"}) {
          constructors.add(new Object[]{config, generatorFn, vectorize});
        }
      }
    }
    return constructors;
  }


  @Test
  public void testGroupBySomeField()
  {
    if (!"segments".equals(segmentsName)) {
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
    }
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0"))
                                          .setVirtualColumns(new NestedFieldVirtualColumn("nest", ".x", "v0"))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    Sequence<ResultRow> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(helper, tempFolder), groupQuery);

    List<ResultRow> results = seq.toList();
    verifyResults(
        groupQuery.getResultRowSignature(),
        results,
        ImmutableList.of(
            new Object[]{null, 8L},
            new Object[]{"100", 2L},
            new Object[]{"200", 2L},
            new Object[]{"300", 4L}
        )
    );
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
