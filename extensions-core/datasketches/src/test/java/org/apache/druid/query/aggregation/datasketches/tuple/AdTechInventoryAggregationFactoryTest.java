/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package org.apache.druid.query.aggregation.datasketches.tuple;

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
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

public class AdTechInventoryAggregationFactoryTest
{

  @Test
  public void makeAggregateCombiner()
  {
    AggregatorFactory aggregatorFactory = new AdTechInventoryAggregatorFactory("", "", "", null, null, null);
    AggregatorFactory combiningFactory = aggregatorFactory.getCombiningFactory();
    AggregateCombiner<ArrayOfDoublesSketch> combiner = combiningFactory.makeAggregateCombiner();

    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update("a", new double[] {1});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update("b", new double[] {1});
    sketch2.update("c", new double[] {1});

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
    EqualsVerifier.forClass(AdTechInventoryAggregatorFactory.class)
                  .withNonnullFields("name", "fieldName")
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
                  new AdTechInventoryAggregatorFactory(
                      ArrayOfDoublesSketchAdTechModule.AD_TECH_INVENTORY,
                      "userCol",
                      "impressionCol",
                      null,
                      1,
                      null
                  )
              )
              .postAggregators(
                  new FieldAccessPostAggregator("a", ArrayOfDoublesSketchAdTechModule.AD_TECH_INVENTORY),
                  new FinalizingFieldAccessPostAggregator("b", ArrayOfDoublesSketchAdTechModule.AD_TECH_INVENTORY)
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ValueType.LONG)
                    .add(ArrayOfDoublesSketchAdTechModule.AD_TECH_INVENTORY, null)
                    .add("a", ValueType.COMPLEX)
                    .add("b", ValueType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
