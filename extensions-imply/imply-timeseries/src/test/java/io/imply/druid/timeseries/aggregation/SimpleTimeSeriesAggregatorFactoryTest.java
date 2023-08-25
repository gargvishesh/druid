/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.aggregation;

import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

public class SimpleTimeSeriesAggregatorFactoryTest
{

  @Test
  public void combine()
  {
    SimpleTimeSeriesContainer vals = SimpleTimeSeriesContainer.createFromInstance(
        new SimpleTimeSeries(
            new ImplyLongArrayList(new long[]{0, 1, 2, 3, 4}),
            new ImplyDoubleArrayList(new double[]{0.0, 0.1, 0.2, 0.3, 0.4}),
            Intervals.of("1970/P1D"),
            1000
        )
    );
    SimpleTimeSeriesContainer nullCon = SimpleTimeSeriesContainer.createFromInstance(null);

    SimpleTimeSeriesAggregatorFactory factory = SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
        "bob",
        null,
        null,
        "notUsed",
        Intervals.of("1970/P1D"),
        1000
    );

    Assert.assertSame(vals, factory.combine(vals, nullCon));
    Assert.assertSame(vals, factory.combine(nullCon, vals));

    Assert.assertSame(nullCon, factory.combine(SimpleTimeSeriesContainer.createFromInstance(null), nullCon));
  }

  @Test
  public void testAggregateCombiner()
  {
    SimpleTimeSeriesContainer[] vals = new SimpleTimeSeriesContainer[]{
        SimpleTimeSeriesContainer.createFromInstance(
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{0, 1, 2}),
                new ImplyDoubleArrayList(new double[]{0.0, 0.1, 0.2}),
                Intervals.of("1970/P1D"),
                1000
            )
        ),
        SimpleTimeSeriesContainer.createFromInstance(
            new SimpleTimeSeries(
                new ImplyLongArrayList(new long[]{3, 4}),
                new ImplyDoubleArrayList(new double[]{0.3, 0.4}),
                Intervals.of("1970/P1D"),
                1000
            )
        )
    };

    SimpleTimeSeriesAggregatorFactory factory = SimpleTimeSeriesAggregatorFactory.getTimeSeriesAggregationFactory(
        "bob",
        null,
        null,
        "notUsed",
        Intervals.of("1970/P1D"),
        1000
    );

    TestObjectColumnSelector columnSelector = new TestObjectColumnSelector<>(vals);

    AggregateCombiner aggregateCombiner = factory.makeAggregateCombiner();

    aggregateCombiner.reset(columnSelector);

    Assert.assertEquals(vals[0], aggregateCombiner.getObject());

    columnSelector.increment();
    aggregateCombiner.fold(columnSelector);

    Assert.assertEquals(
        SimpleTimeSeriesContainer.createFromInstance(
          new SimpleTimeSeries(
              new ImplyLongArrayList(new long[]{0, 1, 2, 3, 4}),
              new ImplyDoubleArrayList(new double[]{0.0, 0.1, 0.2, 0.3, 0.4}),
              Intervals.of("1970/P1D"),
              1000
          )
        ),
        aggregateCombiner.getObject()
    );

    aggregateCombiner.reset(columnSelector);

    Assert.assertEquals(vals[1], aggregateCombiner.getObject());
  }
}
