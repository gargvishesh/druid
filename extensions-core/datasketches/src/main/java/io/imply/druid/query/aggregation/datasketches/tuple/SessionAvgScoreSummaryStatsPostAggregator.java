/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.query.aggregation.ImplyAggregationUtil;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketchIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class SessionAvgScoreSummaryStatsPostAggregator implements PostAggregator
{
  /**
   * The order of these stats matters as the ordinal is used for caching.
   */
  public enum StatType
  {
    N(SummaryStatistics::getN),
    MIN(SummaryStatistics::getMin),
    MAX(SummaryStatistics::getMax),
    SUM(SummaryStatistics::getSum),
    MEAN(SummaryStatistics::getMean),
    GEOMETRIC_MEAN(SummaryStatistics::getGeometricMean),
    VARIANCE(SummaryStatistics::getVariance),
    POPULATION_VARIANCE(SummaryStatistics::getPopulationVariance),
    SECOND_MOMENT(SummaryStatistics::getSecondMoment),
    SUM_OF_SQUARES(SummaryStatistics::getSumsq),
    STD_DEVIATION(SummaryStatistics::getStandardDeviation);

    private final Function<SummaryStatistics, Number> fn;

    StatType(Function<SummaryStatistics, Number> fn)
    {
      this.fn = fn;
    }

    public Number getSummaryStat(SummaryStatistics stats)
    {
      return fn.apply(stats);
    }
  }

  private final String name;
  private final PostAggregator field;
  private final StatType stat;
  private final boolean useAverage;

  public SessionAvgScoreSummaryStatsPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("stat") StatType stat,
      @JsonProperty("useAverage") Boolean useAverage
  )
  {
    this.name = name;
    this.field = field;
    this.stat = stat;
    this.useAverage = useAverage == null || useAverage;
  }

  @Nullable
  @Override
  @JsonProperty("name")
  public String getName()
  {
    return name;
  }

  @JsonProperty("field")
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty("stat")
  public StatType getStat()
  {
    return stat;
  }

  @JsonProperty("useAverage")
  public boolean isUseAverage()
  {
    return useAverage;
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder builder = new CacheKeyBuilder(ImplyAggregationUtil.STRING_BASED)
        .appendString("svsStats")
        .appendInt(stat.ordinal())
        .appendCacheable(field);
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
    return Comparator.comparingDouble(o -> (Double) o);
  }

  @Nullable
  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) field.compute(combinedAggregators);

    SummaryStatistics summaryStats = new SummaryStatistics();
    final ArrayOfDoublesSketchIterator iterator = sketch.iterator();
    if (useAverage) {
      while (iterator.next()) {
        final double[] vals = iterator.getValues();
        summaryStats.addValue(vals[0] / vals[1]);
      }
    } else {
      while (iterator.next()) {
        final double[] vals = iterator.getValues();
        summaryStats.addValue(vals[0]);
      }
    }

    return stat.getSummaryStat(summaryStats).doubleValue();
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @Nullable
  @Override
  public ColumnType getType(ColumnInspector signature)
  {
    return ColumnType.DOUBLE;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SessionAvgScoreSummaryStatsPostAggregator)) {
      return false;
    }
    SessionAvgScoreSummaryStatsPostAggregator that = (SessionAvgScoreSummaryStatsPostAggregator) o;
    return Objects.equals(name, that.name) && Objects.equals(field, that.field) && stat == that.stat;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, stat);
  }

  @Override
  public String toString()
  {
    return "SessionAvgScoreSummaryStatsPostAggregator{" +
           "name='" + name + '\'' +
           ", field=" + field +
           ", stat=" + stat +
           '}';
  }
}
