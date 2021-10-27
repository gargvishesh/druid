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
import com.google.common.base.Preconditions;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketchIterator;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@EverythingIsNonnullByDefault
public class SampledAvgScoreToHistogramPostAggregator implements PostAggregator
{
  private final String name;
  private final PostAggregator field;
  private final double[] splitPoints;

  @JsonCreator
  public SampledAvgScoreToHistogramPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("splitPoints") final double[] splitPoints)
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.splitPoints = Preconditions.checkNotNull(splitPoints, "splitPoints is null");
    for (int i = 1; i < splitPoints.length; i++) {
      if (splitPoints[i] < splitPoints[i - 1]) {
        throw new IAE("The split points for the histogram should be sorted");
      }
    }
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty
  public double[] getSplitPoints()
  {
    return splitPoints;
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder builder = new CacheKeyBuilder((byte) -3)
        .appendCacheable(field)
        .appendDoubleArray(splitPoints);
    return builder.build();
  }

  @Override
  public Set<String> getDependentFields()
  {
    return field.getDependentFields();
  }

  @Override
  public Comparator getComparator()
  {
    throw new IAE("Comparing histograms is not supported");
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) field.compute(combinedAggregators);
    ArrayOfDoublesSketchIterator iterator = sketch.iterator();
    long[] counters = new long[splitPoints.length + 1];
    while (iterator.next()) {
      double[] values = iterator.getValues();
      double avg = (values[0] / values[1]);
      boolean found = false;
      for (int j = 0; j < splitPoints.length; j++) {
        if (avg < splitPoints[j]) {
          counters[j] += 1;
          found = true;
          break;
        }
      }
      if (!found) {
        counters[splitPoints.length] += 1;
      }
    }
    double sketchTheta = sketch.getTheta();
    for (int i = 0; i < counters.length; i++) {
      counters[i] = (long) Math.floor(counters[i] / sketchTheta); // compute the acutal extrapolated number
    }
    return counters;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Nullable
  @Override
  public ColumnType getType(ColumnInspector signature)
  {
    return ColumnType.LONG_ARRAY;
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           ", field=" + field +
           ", splitPoints=" + Arrays.toString(splitPoints) +
           "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SampledAvgScoreToHistogramPostAggregator that = (SampledAvgScoreToHistogramPostAggregator) o;
    return Objects.equals(name, that.getName()) &&
           Objects.equals(field, that.getField()) &&
           Arrays.equals(splitPoints, that.splitPoints);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, Arrays.hashCode(splitPoints));
  }
}
