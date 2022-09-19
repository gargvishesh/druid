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
import org.apache.druid.common.guava.GuavaUtils;
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
public class SessionAvgScoreAggregatorFactory extends ArrayOfDoublesSketchAggregatorFactory
{
  public static final Integer DEFAULT_TARGET_SAMPLES = 1 << 16;
  private final String sessionColumn;
  private final String scoreColumn;
  private final boolean zeroFiltering;

  public SessionAvgScoreAggregatorFactory(
      String name,
      String sessionColumn,
      String scoreColumn,
      Integer sampleSize,
      boolean zeroFiltering
  )
  {
    super(name,
          sessionColumn,
          sampleSize,
          ImmutableList.of(scoreColumn, "count"),
          2);
    this.sessionColumn = sessionColumn;
    this.scoreColumn = scoreColumn;
    this.zeroFiltering = zeroFiltering;
  }

  @JsonCreator
  public static SessionAvgScoreAggregatorFactory getAvgSessionScoresAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("sessionColumn") final String sessionColumn,
      @JsonProperty("scoreColumn") final String scoreColumn,
      @JsonProperty("targetSamples") @Nullable final Integer targetSamples,
      @JsonProperty("zeroFiltering") @Nullable final Boolean zeroFiltering
  )
  {
    if (sessionColumn == null || scoreColumn == null) {
      throw new IAE("Must have a valid, non-null sessionColumn and scoreColumn");
    }
    int normalizedSampleSize = DEFAULT_TARGET_SAMPLES;
    if (targetSamples != null) {
      normalizedSampleSize = Math.max(1, Integer.highestOneBit(targetSamples - 1) << 1); // nearest power of 2 >= targetSamples
    }

    boolean zeroFilteringArg = GuavaUtils.firstNonNull(zeroFiltering, false);

    return new SessionAvgScoreAggregatorFactory(name,
                                                sessionColumn,
                                                scoreColumn,
                                                normalizedSampleSize,
                                                zeroFilteringArg);
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
    BaseDoubleColumnValueSelector scoreSelector = metricFactory.makeColumnValueSelector(getScoreColumn());
    valueSelectors.add(scoreSelector);
    valueSelectors.add(new OneDoubleColumnValueSelector(scoreSelector, zeroFiltering));
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
    BaseDoubleColumnValueSelector scoreSelector = metricFactory.makeColumnValueSelector(getScoreColumn());
    valueSelectors.add(scoreSelector);
    valueSelectors.add(new OneDoubleColumnValueSelector(scoreSelector, zeroFiltering));
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
    return ImmutableList.of(sessionColumn, scoreColumn);
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new SessionAvgScoreAggregatorFactory(
        newName,
        getSessionColumn(),
        getScoreColumn(),
        getNominalEntries(),
        isZeroFiltering()
    );
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    throw new UOE(StringUtils.format(
        "GroupByStrategyV1 is not supported for %s aggregator",
        ImplyArrayOfDoublesSketchModule.SESSION_AVG_SCORE
    ));
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return super.getName();
  }

  @JsonProperty
  public String getSessionColumn()
  {
    return sessionColumn;
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

  @JsonProperty
  public boolean isZeroFiltering()
  {
    return zeroFiltering;
  }

  @Override
  public byte[] getCacheKey()
  {
    // using id=-2 to avoid conflict with new aggregators and avoid change in AggrregatorUtil; hack and not a trend
    final CacheKeyBuilder builder = new CacheKeyBuilder((byte) -2)
        .appendString(getName())
        .appendString(getSessionColumn())
        .appendString(getScoreColumn())
        .appendInt(getTargetSamples());
    return builder.build();
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{"
           + "name=" + getName()
           + ", sessionColumn=" + getSessionColumn()
           + ", scoreColumn=" + getScoreColumn()
           + ", targetSamples=" + getTargetSamples()
           + ", zeroFiltering=" + isZeroFiltering()
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
    SessionAvgScoreAggregatorFactory that = (SessionAvgScoreAggregatorFactory) o;
    return Objects.equals(getName(), that.getName()) &&
           Objects.equals(getSessionColumn(), that.getSessionColumn()) &&
           Objects.equals(getScoreColumn(), that.getScoreColumn()) &&
           this.getTargetSamples() == that.getTargetSamples() &&
           this.isZeroFiltering() == that.isZeroFiltering() &&
           super.equals(o);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getName(),
                        getSessionColumn(),
                        getScoreColumn(),
                        getTargetSamples(),
                        isZeroFiltering(),
                        super.hashCode());
  }

  private static class OneDoubleColumnValueSelector
      implements BaseDoubleColumnValueSelector
  {
    private final BaseDoubleColumnValueSelector scoreSelector;
    private final boolean zeroFiltering;

    public OneDoubleColumnValueSelector(BaseDoubleColumnValueSelector scoreSelector, boolean zeroFiltering)
    {
      this.scoreSelector = scoreSelector;
      this.zeroFiltering = zeroFiltering;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("value", 1);
    }

    @Override
    public double getDouble()
    {
      if (zeroFiltering && !scoreSelector.isNull() && scoreSelector.getDouble() == 0) {
        return 0;
      }

      return 1;
    }

    @Override
    public boolean isNull()
    {
      return false;
    }
  }
}
