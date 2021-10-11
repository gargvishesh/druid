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
import com.google.common.collect.ImmutableList;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchBuildAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchBuildBufferAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.NoopArrayOfDoublesSketchAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.NoopArrayOfDoublesSketchBufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@EverythingIsNonnullByDefault
public class SampledAvgScoreAggregatorFactory extends ArrayOfDoublesSketchAggregatorFactory
{
  public static final Integer DEFAULT_TARGET_SAMPLES = 1 << 16;
  private final String sampleColumn;
  private final String scoreColumn;

  public SampledAvgScoreAggregatorFactory(
      String name,
      String sampleColumn,
      String scoreColumn,
      Integer sampleSize
  )
  {
    super(name,
          sampleColumn,
          sampleSize,
          ImmutableList.of(scoreColumn, "count"),
          2);
    this.sampleColumn = sampleColumn;
    this.scoreColumn = scoreColumn;
  }

  @JsonCreator
  public static SampledAvgScoreAggregatorFactory getAvgSessionScoresAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("sampleColumn") final String sampleColumn,
      @JsonProperty("scoreColumn") final String scoreColumn,
      @JsonProperty("targetSamples") @Nullable final Integer targetSamples
  )
  {
    if (sampleColumn == null || scoreColumn == null) {
      throw new IAE("Must have a valid, non-null sampleColumn and scoreColumn");
    }
    int normalizedSampleSize = DEFAULT_TARGET_SAMPLES;
    if (targetSamples != null) {
      normalizedSampleSize = Math.max(1, Integer.highestOneBit(targetSamples - 1) << 1); // nearest power of 2 >= targetSamples
    }

    return new SampledAvgScoreAggregatorFactory(name,
                                                sampleColumn,
                                                scoreColumn,
                                                normalizedSampleSize);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {
    // input is raw data (key and array of values), use build aggregator
    final DimensionSelector keySelector = metricFactory
        .makeDimensionSelector(new DefaultDimensionSpec(getFieldName(), getFieldName()));
    if (DimensionSelector.isNilSelector(keySelector)) {
      return new NoopArrayOfDoublesSketchAggregator(getNumberOfValues());
    }
    final List<BaseDoubleColumnValueSelector> valueSelectors = new ArrayList<>();
    valueSelectors.add(metricFactory.makeColumnValueSelector(getScoreColumn()));
    valueSelectors.add(new OneDoubleColumnValueSelector());
    return new ArrayOfDoublesSketchBuildAggregator(keySelector, valueSelectors, getNominalEntries());
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    // input is raw data (key and array of values), use build aggregator
    final DimensionSelector keySelector = metricFactory
        .makeDimensionSelector(new DefaultDimensionSpec(getFieldName(), getFieldName()));
    if (DimensionSelector.isNilSelector(keySelector)) {
      return new NoopArrayOfDoublesSketchBufferAggregator(getNumberOfValues());
    }
    final List<BaseDoubleColumnValueSelector> valueSelectors = new ArrayList<>();
    valueSelectors.add(metricFactory.makeColumnValueSelector(getScoreColumn()));
    valueSelectors.add(new OneDoubleColumnValueSelector());
    return new ArrayOfDoublesSketchBuildBufferAggregator(
        keySelector,
        valueSelectors,
        getNominalEntries(),
        getMaxIntermediateSizeWithNulls()
    );
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable final Object object)
  {
    if (object == null) {
      return null;
    }
    ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) object;
    return sketch.getEstimate();
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of(sampleColumn, scoreColumn);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    throw new UOE(StringUtils.format("GroupByStrategyV1 is not supported for %s aggregator",
                                     ImplyArrayOfDoublesSketchModule.SAMPLED_AVG_SCORE
    ));
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return super.getName();
  }

  @JsonProperty
  public String getSampleColumn()
  {
    return sampleColumn;
  }

  @JsonProperty
  public String getScoreColumn()
  {
    return scoreColumn;
  }

  @JsonProperty
  public int getTargetSamples()
  {
    return super.getNominalEntries();
  }

  @Override
  public byte[] getCacheKey()
  {
    // using id=-2 to avoid conflict with new aggregators and avoid change in AggrregatorUtil; hack and not a trend
    final CacheKeyBuilder builder = new CacheKeyBuilder((byte) -2)
        .appendString(getName())
        .appendString(getSampleColumn())
        .appendString(getScoreColumn())
        .appendInt(getTargetSamples());
    return builder.build();
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{"
           + "name=" + getName()
           + ", sessionIdColumn=" + getSampleColumn()
           + ", sessionScoreColumn=" + getScoreColumn()
           + ", targetSamples=" + getTargetSamples()
           + "}";
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SampledAvgScoreAggregatorFactory that = (SampledAvgScoreAggregatorFactory) o;
    return Objects.equals(getName(), that.getName()) &&
           Objects.equals(getSampleColumn(), that.getSampleColumn()) &&
           Objects.equals(getScoreColumn(), that.getScoreColumn()) &&
           this.getTargetSamples() == that.getTargetSamples() &&
           super.equals(o);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getName(),
                        getSampleColumn(),
                        getScoreColumn(),
                        getTargetSamples(),
                        super.hashCode());
  }

  private static class OneDoubleColumnValueSelector
      implements BaseDoubleColumnValueSelector
  {
    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("value", 1);
    }

    @Override
    public double getDouble()
    {
      return 1;
    }

    @Override
    public boolean isNull()
    {
      return false;
    }
  }
}
