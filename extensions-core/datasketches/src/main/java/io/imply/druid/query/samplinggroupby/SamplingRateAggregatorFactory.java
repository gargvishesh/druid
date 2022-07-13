/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NoopAggregator;
import org.apache.druid.query.aggregation.NoopBufferAggregator;
import org.apache.druid.query.aggregation.NoopVectorAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;

/**
 * No-op aggregator to support SAMPLING_RATE() syntax.
 */
@EverythingIsNonnullByDefault
public class SamplingRateAggregatorFactory extends AggregatorFactory
{
  private final String name;

  @JsonCreator
  public SamplingRateAggregatorFactory(@JsonProperty("name") String name)
  {
    this.name = name;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return NoopAggregator.instance();
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return NoopBufferAggregator.instance();
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    return NoopVectorAggregator.instance();
  }

  @Override
  public Comparator getComparator()
  {
    return Comparator.naturalOrder();
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    return null;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SamplingRateAggregatorFactory(getName());
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return ImmutableList.of();
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 4;
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.DOUBLE;
  }

  @Override
  public ColumnType getResultType()
  {
    return getIntermediateType();
  }
}
