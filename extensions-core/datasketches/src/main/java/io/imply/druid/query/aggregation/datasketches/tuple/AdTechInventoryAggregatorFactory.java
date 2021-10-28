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
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchAggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class AdTechInventoryAggregatorFactory extends ArrayOfDoublesSketchAggregatorFactory
{
  @Nullable
  private final String userColumn;
  @Nullable
  private final String adTechInventoryColumn;
  @Nullable
  private final String impressionColumn;
  @Nullable
  private final Integer frequencyCap;

  public AdTechInventoryAggregatorFactory(
      String name,
      @Nullable String userColumn,
      @Nullable String adTechInventoryColumn,
      @Nullable String impressionColumn,
      @Nullable Integer frequencyCap,
      Integer sampleSize
  )
  {
    super(name,
          GuavaUtils.firstNonNull(userColumn, adTechInventoryColumn),
          sampleSize,
          impressionColumn == null ? null : ImmutableList.of(impressionColumn),
          null);
    this.userColumn = userColumn;
    this.adTechInventoryColumn = adTechInventoryColumn;
    this.impressionColumn = impressionColumn;
    this.frequencyCap = frequencyCap;
  }

  @JsonCreator
  public static AdTechInventoryAggregatorFactory getAdTechImpressionCountAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("userColumn") @Nullable final String userColumn,
      @JsonProperty("impressionColumn") @Nullable final String impressionColumn,
      @JsonProperty("adTechInventoryColumn") @Nullable final String adTechInventoryColumn,
      @JsonProperty("frequencyCap") @Nullable final Integer frequencyCap,
      @JsonProperty("sampleSize") @Nullable final Integer sampleSize
  )
  {
    boolean isRawData = userColumn != null && impressionColumn != null;
    boolean isPreBuiltData = userColumn == null && impressionColumn == null && adTechInventoryColumn != null;
    if (isPreBuiltData == isRawData) { // the input must either be raw or preBuilt exclusively
      throw new IAE("Must exclusively have a valid, non-null (userColumn, impressionColumn) or "
                    + "adTechInventoryColumn");
    }
    int normalizedSampleSize = 1 << 16; // sample size defaults to 64k
    if (sampleSize != null) {
      normalizedSampleSize = Math.max(1, Integer.highestOneBit(sampleSize - 1) << 1); // nearest power of 2 >= sampleSize
    }

    return new AdTechInventoryAggregatorFactory(name,
                                                userColumn,
                                                adTechInventoryColumn,
                                                impressionColumn,
                                                frequencyCap,
                                                normalizedSampleSize);
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable final Object object)
  {
    if (object == null) {
      return null;
    }
    ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) object;
    return getCumulativeImpressionsWithFreqCap(sketch, frequencyCap);
  }

  private double getCumulativeImpressionsWithFreqCap(ArrayOfDoublesSketch sketch, @Nullable Integer frequencyCap)
  {
    if (frequencyCap == null) {
      return Arrays.stream(sketch.getValues()).mapToDouble(value -> value[0]).sum() / sketch.getTheta();
    }
    return Arrays.stream(sketch.getValues()).mapToInt(value -> (int) Math.min(value[0], frequencyCap)).sum() / sketch.getTheta();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new AdTechInventoryAggregatorFactory(getName(), null, getName(), null, frequencyCap, getSampleSize());
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    throw new UOE(StringUtils.format("GroupByStrategyV1 is not supported for %s aggregator",
                                     ImplyArrayOfDoublesSketchModule.AD_TECH_INVENTORY
    ));
  }

  @Override
  public Comparator<ArrayOfDoublesSketch> getComparator()
  {
    return Comparator
        .nullsFirst(Comparator.comparingDouble(sketch -> getCumulativeImpressionsWithFreqCap(sketch, frequencyCap)));
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return super.getName();
  }

  @Nullable
  @JsonProperty
  public String getUserColumn()
  {
    return userColumn;
  }

  @Nullable
  @JsonProperty
  public String getImpressionColumn()
  {
    return impressionColumn;
  }

  @Nullable
  @JsonProperty
  public String getAdTechInventoryColumn()
  {
    return adTechInventoryColumn;
  }

  @JsonProperty
  public int getSampleSize()
  {
    return super.getNominalEntries();
  }

  @Nullable
  @JsonProperty
  public Integer getFrequencyCap()
  {
    return frequencyCap;
  }

  @Override
  public byte[] getCacheKey()
  {
    // using id=-1 to avoid conflict with new aggregators and avoid change in AggrregatorUtil; hack and not a trend
    final CacheKeyBuilder builder = new CacheKeyBuilder((byte) -1)
        .appendString(getName())
        .appendString(getUserColumn())
        .appendString(getImpressionColumn())
        .appendString(getAdTechInventoryColumn())
        .appendString(String.valueOf(getFrequencyCap()))
        .appendInt(getSampleSize());
    return builder.build();
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{"
           + "name=" + getName()
           + ", userColumn=" + getUserColumn()
           + ", impressionColumn=" + getImpressionColumn()
           + ", adTechInventoryColumn=" + getAdTechInventoryColumn()
           + ", frequcencyCap=" + getFrequencyCap()
           + ", sampleSize=" + getSampleSize()
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
    AdTechInventoryAggregatorFactory that = (AdTechInventoryAggregatorFactory) o;
    return Objects.equals(getName(), that.getName()) &&
           Objects.equals(getUserColumn(), that.getUserColumn()) &&
           Objects.equals(getImpressionColumn(), that.getImpressionColumn()) &&
           Objects.equals(getAdTechInventoryColumn(), that.getAdTechInventoryColumn()) &&
           Objects.equals(getFrequencyCap(), that.getFrequencyCap()) &&
           this.getSampleSize() == that.getSampleSize() &&
           super.equals(o);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getName(),
                        getUserColumn(),
                        getImpressionColumn(),
                        getAdTechInventoryColumn(),
                        getFrequencyCap(),
                        getSampleSize(),
                        super.hashCode());
  }

}
