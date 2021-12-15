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
import com.google.common.primitives.Longs;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketchIterator;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@EverythingIsNonnullByDefault
public class SessionAvgScoreToHistogramFilteringPostAggregator implements PostAggregator
{
  private final String name;
  private final PostAggregator field;
  private final double[] splitPoints;
  private final int[] filterBuckets;

  @JsonCreator
  public SessionAvgScoreToHistogramFilteringPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("splitPoints") final double[] splitPoints,
      @JsonProperty("filterBuckets") final int[] filterBuckets)
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.splitPoints = Preconditions.checkNotNull(splitPoints, "splitPoints is null");
    for (int i = 1; i < splitPoints.length; i++) {
      if (splitPoints[i] < splitPoints[i - 1]) {
        throw new IAE("The split points for the histogram should be sorted");
      }
    }
    this.filterBuckets = filterBuckets;
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

  @JsonProperty
  public int[] getFilterBuckets()
  {
    return filterBuckets;
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
    if (sketch == null) {
      return null;
    }

    ArrayOfDoublesSketchIterator iterator = sketch.iterator();
    List<Long> hashKeys = new ArrayList<>();
    Set<Integer> filterBucketSet = Arrays.stream(filterBuckets).boxed().collect(Collectors.toSet());
    while (iterator.next()) {
      double[] values = iterator.getValues();
      double avg = values[0] == 0 ? 0 : (values[0] / values[1]); // if num == 0, then we want the result to be zero (even if den is 0)
      boolean found = false;
      for (int j = 0; j < splitPoints.length; j++) {
        if (avg < splitPoints[j]) {
          if (filterBucketSet.contains(j)) {
            hashKeys.add(iterator.getKey());
          }
          found = true;
          break;
        }
      }
      if (!found && (filterBucketSet.contains(splitPoints.length))) {
        hashKeys.add(iterator.getKey());
      }
    }
    ByteBuffer longBuffer = ByteBuffer.allocate(hashKeys.size() * Long.BYTES);
    longBuffer.asLongBuffer().put(Longs.toArray(hashKeys));
    return new String(StringUtils.encodeBase64(longBuffer.array()), StandardCharsets.UTF_8);
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
    return ColumnType.STRING;
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
           ", filterBuckets=" + Arrays.toString(filterBuckets) +
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
    final SessionAvgScoreToHistogramFilteringPostAggregator that = (SessionAvgScoreToHistogramFilteringPostAggregator) o;
    return Objects.equals(name, that.getName()) &&
           Objects.equals(field, that.getField()) &&
           Arrays.equals(splitPoints, that.splitPoints) &&
           Arrays.equals(filterBuckets, that.filterBuckets);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, Arrays.hashCode(splitPoints), Arrays.hashCode(filterBuckets));
  }
}
