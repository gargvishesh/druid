/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchBuildAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class SessionAvgScoreToHistogramFilteringPostAggregatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    PostAggregator there = new SessionAvgScoreToHistogramFilteringPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        new double[]{0.25, 0.75},
        new int[]{0}
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    SessionAvgScoreToHistogramFilteringPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        SessionAvgScoreToHistogramFilteringPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new SessionAvgScoreToHistogramFilteringPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        new double[]{0.25, 0.75},
        new int[]{0}
    );

    Assert.assertEquals(
        "SessionAvgScoreToHistogramFilteringPostAggregator{name='post', "
        + "field=FieldAccessPostAggregator{name='field1', fieldName='sketch'}, splitPoints=[0.25, 0.75], filterBuckets=[0]}",
        postAgg.toString()
    );
  }

  @Test
  public void testComparator()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Comparing histograms is not supported");
    final PostAggregator postAgg = new SessionAvgScoreToHistogramFilteringPostAggregator(
        "post",
        new FieldAccessPostAggregator("field1", "sketch"),
        new double[]{0.25, 0.75},
        new int[0]
    );
    postAgg.getComparator();
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(SessionAvgScoreToHistogramFilteringPostAggregator.class)
                  .withNonnullFields("name", "field", "splitPoints", "filterBuckets")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void emptySketch()
  {
    final Aggregator agg = new ArrayOfDoublesSketchBuildAggregator(DimensionSelector.constant(null), Collections.singletonList(null), 16);

    final Map<String, Object> fields = new HashMap<>();
    fields.put("sketch", agg.get());

    final PostAggregator postAgg = new SessionAvgScoreToHistogramFilteringPostAggregator(
        "histogram",
        new FieldAccessPostAggregator("field", "sketch"),
        new double[] {3.5},
        new int[]{0}
    );

    final String histogram = (String) postAgg.compute(fields);
    Assert.assertEquals("", histogram);
  }

  @Test
  public void splitPoints()
  {
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
                                                                                     .setNumberOfValues(2)
                                                                                     .build();
    IntStream.range(1, 7).forEach(i -> sketch.update(i, new double[]{i, 1}));
    final Map<String, Object> fields = new HashMap<>();
    fields.put("sketch", sketch);

    PostAggregator postAgg = new SessionAvgScoreToHistogramFilteringPostAggregator(
        "histogram",
        new FieldAccessPostAggregator("field", "sketch"),
        new double[] {3.5}, // splits distribution into two bins of equal mass
        new int[]{0}
    );

    String histogram = (String) postAgg.compute(fields);
    Assert.assertNotNull(histogram);
    Assert.assertEquals("Hp1wgRL8l8MFoYa9y335FV1pBtrBs0C6", histogram);

    postAgg = new SessionAvgScoreToHistogramFilteringPostAggregator(
        "histogram",
        new FieldAccessPostAggregator("field", "sketch"),
        new double[] {3.5}, // splits distribution into two bins of equal mass
        new int[]{1}
    );

    histogram = (String) postAgg.compute(fields);
    Assert.assertNotNull(histogram);
    Assert.assertEquals("CD3byeEu3kAUzJFGcnMyvRC8mPsTIRb+", histogram);
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
                  new SessionAvgScoreAggregatorFactory("sketch", "col", "score", 128, false)
              )
              .postAggregators(
                  new SessionAvgScoreToHistogramFilteringPostAggregator(
                      "a",
                      new FieldAccessPostAggregator("field", "sketch"),
                      new double[] {3.5},
                      new int[]{0}
                  )
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("sketch", null)
                    .add("a", ColumnType.STRING)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
