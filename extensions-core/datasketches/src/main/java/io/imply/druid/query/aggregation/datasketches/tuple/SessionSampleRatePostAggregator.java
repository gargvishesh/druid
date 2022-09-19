/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.query.aggregation.ImplyAggregationUtil;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchUnaryPostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import java.util.Comparator;
import java.util.Map;

public class SessionSampleRatePostAggregator extends ArrayOfDoublesSketchUnaryPostAggregator
{

  @JsonCreator
  public SessionSampleRatePostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field
  )
  {
    super(name, field);
  }

  @Override
  public Double compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) getField().compute(combinedAggregators);
    return sketch == null ? null : sketch.getTheta();
  }

  @Override
  public ColumnType getType(ColumnInspector signature)
  {
    return ColumnType.DOUBLE;
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return Comparator.naturalOrder();
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(ImplyAggregationUtil.SESSION_SAMPLE_RATE_CACHE_ID)
        .appendCacheable(getField())
        .build();
  }
}
