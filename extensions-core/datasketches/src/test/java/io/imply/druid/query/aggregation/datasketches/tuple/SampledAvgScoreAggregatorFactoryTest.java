/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

public class SampledAvgScoreAggregatorFactoryTest
{
  @Test
  public void makeAggregateCombiner()
  {
    AggregatorFactory aggregatorFactory = new SampledAvgScoreAggregatorFactory("", "", "", null);
    AggregatorFactory combiningFactory = aggregatorFactory.getCombiningFactory();
    AggregateCombiner<ArrayOfDoublesSketch> combiner = combiningFactory.makeAggregateCombiner();

    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch1.update("a", new double[] {1, 1});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().setNumberOfValues(2).build();
    sketch2.update("b", new double[] {1, 1});
    sketch2.update("c", new double[] {1, 1});

    TestObjectColumnSelector<ArrayOfDoublesSketch> selector = new TestObjectColumnSelector<ArrayOfDoublesSketch>(new ArrayOfDoublesSketch[] {sketch1, sketch2});

    combiner.reset(selector);
    Assert.assertEquals(1, combiner.getObject().getEstimate(), 0);

    selector.increment();
    combiner.fold(selector);
    Assert.assertEquals(3, combiner.getObject().getEstimate(), 0);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SampledAvgScoreAggregatorFactory.class)
                  .withNonnullFields("name", "fieldName", "sampleColumn", "scoreColumn")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count"),
                  new SampledAvgScoreAggregatorFactory(
                      ImplyArrayOfDoublesSketchModule.SAMPLED_AVG_SCORE,
                      "sampleCol",
                      "scoreCol",
                      null
                  )
              )
              .postAggregators(
                  new FieldAccessPostAggregator("a", ImplyArrayOfDoublesSketchModule.SAMPLED_AVG_SCORE),
                  new FinalizingFieldAccessPostAggregator("b", ImplyArrayOfDoublesSketchModule.SAMPLED_AVG_SCORE)
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add(ImplyArrayOfDoublesSketchModule.SAMPLED_AVG_SCORE, null)
                    .add("a", ArrayOfDoublesSketchModule.BUILD_TYPE)
                    .add("b", ColumnType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
