/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.junit.Assert;
import org.junit.Test;

public class SamplingGroupByQueryTest
{
  @SuppressWarnings("EqualsWithItself")
  @Test
  public void testResultOrderingWithTimeGranularity()
  {
    SamplingGroupByQuery samplingGroupByQuery = SamplingGroupByQuery
        .builder()
        .setDimensions(DefaultDimensionSpec.of("d"))
        .setGranularity(Granularities.SECOND)
        .setDataSource("dummy")
        .setInterval(BaseCalciteQueryTest.querySegmentSpec(Intervals.ETERNITY))
        .build();
    Ordering<ResultRow> ordering = samplingGroupByQuery.getResultOrdering();
    ResultRow resultRow1 = ResultRow.of(0L, 1L, "hello", 10L);
    ResultRow resultRow2 = ResultRow.of(1L, 2L, "world", 10L);
    ResultRow resultRow3 = ResultRow.of(1L, 2L, "world1", 10L);
    Assert.assertTrue(ordering.compare(resultRow1, resultRow2) < 0); // time comparison
    Assert.assertTrue(ordering.compare(resultRow2, resultRow1) > 0); // reverse time
    Assert.assertEquals(0, ordering.compare(resultRow1, resultRow1)); // same row
    Assert.assertTrue(ordering.compare(resultRow2, resultRow3) < 0); // same time and hash but diff dim value
  }

  @SuppressWarnings("EqualsWithItself")
  @Test
  public void testResultOrdering()
  {
    SamplingGroupByQuery samplingGroupByQuery = SamplingGroupByQuery
        .builder()
        .setDimensions(DefaultDimensionSpec.of("d"))
        .setGranularity(Granularities.ALL)
        .setDataSource("dummy")
        .setInterval(BaseCalciteQueryTest.querySegmentSpec(Intervals.ETERNITY))
        .build();
    Ordering<ResultRow> ordering = samplingGroupByQuery.getResultOrdering();
    ResultRow resultRow1 = ResultRow.of(1L, "hello", 10L);
    ResultRow resultRow2 = ResultRow.of(2L, "world", 10L);
    ResultRow resultRow3 = ResultRow.of(2L, "world1", 10L);
    Assert.assertTrue(ordering.compare(resultRow1, resultRow2) < 0); // hash comparison
    Assert.assertTrue(ordering.compare(resultRow2, resultRow1) > 0); // reverse hash
    Assert.assertEquals(0, ordering.compare(resultRow1, resultRow1)); // same row
    Assert.assertTrue(ordering.compare(resultRow2, resultRow3) < 0); // same hash but diff dim value
  }

  @SuppressWarnings("EqualsWithItself")
  @Test
  public void testIntermediateGroupDeduplicationOrderingWithTimeGranularity()
  {
    SamplingGroupByQuery samplingGroupByQuery = SamplingGroupByQuery
        .builder()
        .setDimensions(DefaultDimensionSpec.of("d"))
        .setGranularity(Granularities.SECOND)
        .setDataSource("dummy")
        .setInterval(BaseCalciteQueryTest.querySegmentSpec(Intervals.ETERNITY))
        .build();
    Ordering<ResultRow> ordering = samplingGroupByQuery.generateIntermediateGroupByQueryOrdering();
    ResultRow resultRow1 = ResultRow.of(0L, 1L, "hello", 10L);
    ResultRow resultRow2 = ResultRow.of(1L, 2L, "world", 10L);
    ResultRow resultRow3 = ResultRow.of(1L, 2L, "world", 5L);
    Assert.assertTrue(ordering.compare(resultRow1, resultRow2) < 0); // time comparison
    Assert.assertTrue(ordering.compare(resultRow2, resultRow1) > 0); // reverse time
    Assert.assertEquals(0, ordering.compare(resultRow1, resultRow1)); // same row
    Assert.assertEquals(0, ordering.compare(resultRow2, resultRow3)); // same hash and dim value so same for de-duping
  }

  @SuppressWarnings("EqualsWithItself")
  @Test
  public void testIntermediateGroupDeduplicationOrdering()
  {
    SamplingGroupByQuery samplingGroupByQuery = SamplingGroupByQuery
        .builder()
        .setDimensions(DefaultDimensionSpec.of("d"))
        .setGranularity(Granularities.ALL)
        .setDataSource("dummy")
        .setInterval(BaseCalciteQueryTest.querySegmentSpec(Intervals.ETERNITY))
        .build();
    Ordering<ResultRow> ordering = samplingGroupByQuery.generateIntermediateGroupByQueryOrdering();
    ResultRow resultRow1 = ResultRow.of(1L, "hello", 10L);
    ResultRow resultRow2 = ResultRow.of(2L, "world", 10L);
    ResultRow resultRow3 = ResultRow.of(2L, "world", 5L);
    ResultRow resultRow4 = ResultRow.of(2L, "world1", 5L);
    Assert.assertTrue(ordering.compare(resultRow1, resultRow2) < 0); // hash comparison
    Assert.assertTrue(ordering.compare(resultRow2, resultRow1) > 0); // reverse hash
    Assert.assertEquals(0, ordering.compare(resultRow1, resultRow1)); // same row
    Assert.assertEquals(0, ordering.compare(resultRow2, resultRow3)); // same hash and dim value so same for de-duping
    Assert.assertTrue(ordering.compare(resultRow3, resultRow4) < 0); // same hash but diff dim value
  }
}
