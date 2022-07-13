/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Accumulators;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class SamplingGroupByMergingSequenceTest
{
  @Test
  public void testAccumulate()
  {
    SamplingGroupByMergingSequence sequence = new SamplingGroupByMergingSequence(
        QueryPlus.wrap(
            new SamplingGroupByQuery(
                TableDataSource.create("dummy"),
                new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY),
                ImmutableList.of(DefaultDimensionSpec.of("dummyCol")),
                VirtualColumns.EMPTY,
                null,
                null,
                null,
                null,
                Granularities.ALL,
                ImmutableMap.of(),
                5
            )
        ),
        Sequences.simple(ImmutableList.of(ResultRow.of(0L, "0", 1L)))
    );

    ArrayList<ResultRow> expectedResult = new ArrayList<>();
    expectedResult.add(ResultRow.of(0L, "0", 1L));
    Assert.assertEquals(
        expectedResult,
        sequence.accumulate(new ArrayList<>(), Accumulators.list())
    );
  }

  @Test
  public void testAccumulateEmptySequence()
  {
    SamplingGroupByMergingSequence sequence = new SamplingGroupByMergingSequence(
        QueryPlus.wrap(
            new SamplingGroupByQuery(
                TableDataSource.create("dummy"),
                new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY),
                ImmutableList.of(DefaultDimensionSpec.of("dummyCol")),
                VirtualColumns.EMPTY,
                null,
                null,
                null,
                null,
                Granularities.ALL,
                ImmutableMap.of(),
                5
            )
        ),
        Sequences.simple(ImmutableList.of())
    );

    ArrayList<ResultRow> expectedResult = new ArrayList<>();
    Assert.assertEquals(
        expectedResult,
        sequence.accumulate(new ArrayList<>(), Accumulators.list())
    );
  }

  @Test
  public void testAccumulateWithGranularity()
  {
    SamplingGroupByMergingSequence sequence = new SamplingGroupByMergingSequence(
        QueryPlus.wrap(
            new SamplingGroupByQuery(
                TableDataSource.create("dummy"),
                new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY),
                ImmutableList.of(DefaultDimensionSpec.of("dummyCol")),
                VirtualColumns.EMPTY,
                null,
                null,
                null,
                null,
                Granularities.SECOND,
                ImmutableMap.of(),
                5
            )
        ),
        Sequences.simple(ImmutableList.of(ResultRow.of(0L, 0L, "0", 1L), ResultRow.of(1L, 0L, "0", 1L)))
    );

    ArrayList<ResultRow> expectedResult = new ArrayList<>();
    expectedResult.add(ResultRow.of(0L, 0L, "0", 1L));
    expectedResult.add(ResultRow.of(1L, 0L, "0", 1L));
    Assert.assertEquals(
        expectedResult,
        sequence.accumulate(new ArrayList<>(), Accumulators.list())
    );
  }

  @Test
  public void testAccumulateWithGranularityAndLimit() throws IOException
  {
    SamplingGroupByMergingSequence sequence = new SamplingGroupByMergingSequence(
        QueryPlus.wrap(
            new SamplingGroupByQuery(
                TableDataSource.create("dummy"),
                new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY),
                ImmutableList.of(DefaultDimensionSpec.of("dummyCol")),
                VirtualColumns.EMPTY,
                null,
                null,
                null,
                null,
                Granularities.SECOND,
                ImmutableMap.of(),
                2
            )
        ),
        Sequences.simple(ImmutableList.of(
            ResultRow.of(0L, 0L, "0", 1L),
            ResultRow.of(0L, 1L, "0", 2L),
            ResultRow.of(0L, 2L, "0", 3L),
            ResultRow.of(1L, 0L, "0", 1L),
            ResultRow.of(1L, 1L, "0", 2L),
            ResultRow.of(1L, 2L, "0", 3L)
        ))
    );

    ArrayList<ResultRow> expectedResult = new ArrayList<>();
    expectedResult.add(ResultRow.of(0L, 0L, "0", 2L));
    expectedResult.add(ResultRow.of(0L, 1L, "0", 2L));
    expectedResult.add(ResultRow.of(1L, 0L, "0", 2L));
    expectedResult.add(ResultRow.of(1L, 1L, "0", 2L));

    ArrayList<ResultRow> result = new ArrayList<>();
    Yielder<ResultRow> yielder = Yielders.each(sequence);
    while (!yielder.isDone()) {
      result.add(yielder.get());
      yielder = yielder.next(null);
    }
    yielder.close();
    Assert.assertEquals(
        expectedResult,
        result
    );
  }
}
