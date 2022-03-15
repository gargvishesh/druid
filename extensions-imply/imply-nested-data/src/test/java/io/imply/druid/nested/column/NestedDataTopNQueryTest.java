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
import io.imply.druid.nested.NestedDataModule;
import io.imply.druid.nested.virtual.NestedFieldVirtualColumn;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.Segment;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

@RunWith(Parameterized.class)
public class NestedDataTopNQueryTest extends InitializedNullHandlingTest
{
  private static final Logger LOG = new Logger(NestedDataTopNQueryTest.class);

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final AggregationTestHelper helper;
  private final BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>> segmentsGenerator;

  public NestedDataTopNQueryTest(
      BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>> segmentGenerator
  )
  {
    NestedDataModule.registerHandlersAndSerde();
    this.helper = AggregationTestHelper.createTopNQueryAggregationTestHelper(
        NestedDataModule.getJacksonModulesList(),
        tempFolder
    );
    this.segmentsGenerator = segmentGenerator;
  }

  @Parameterized.Parameters(name = "segments = {0}")
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

    for (BiFunction<AggregationTestHelper, TemporaryFolder, List<Segment>> generatorFn : segmentsGenerators) {
      constructors.add(new Object[]{generatorFn});
    }
    return constructors;
  }

  @Test
  public void testGroupBySomeField()
  {
    TopNQuery topN = new TopNQueryBuilder().dataSource("test_datasource")
                                           .granularity(Granularities.ALL)
                                           .intervals(Collections.singletonList(Intervals.ETERNITY))
                                           .dimension(DefaultDimensionSpec.of("v0"))
                                           .virtualColumns(new NestedFieldVirtualColumn("nest", ".x", "v0"))
                                           .aggregators(new CountAggregatorFactory("count"))
                                           .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                                           .threshold(10)
                                           .build();


    Sequence<Result<TopNResultValue>> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(helper, tempFolder), topN);

    Sequence<Object[]> resultsSeq = new TopNQueryQueryToolChest(new TopNQueryConfig()).resultsAsArrays(topN, seq);

    List<Object[]> results = resultsSeq.toList();

    verifyResults(
        results,
        ImmutableList.of(
            new Object[]{1609459200000L, null, 8L},
            new Object[]{1609459200000L, "100", 2L},
            new Object[]{1609459200000L, "200", 2L},
            new Object[]{1609459200000L, "300", 4L}
        )
    );
  }

  private static void verifyResults(List<Object[]> results, List<Object[]> expected)
  {
    Assert.assertEquals(expected.size(), results.size());

    for (int i = 0; i < expected.size(); i++) {
      LOG.info("result #%d, %s", i, Arrays.toString(results.get(i)));
      Assert.assertArrayEquals(
          StringUtils.format("result #%d", i + 1),
          expected.get(i),
          results.get(i)
      );
    }
  }
}
