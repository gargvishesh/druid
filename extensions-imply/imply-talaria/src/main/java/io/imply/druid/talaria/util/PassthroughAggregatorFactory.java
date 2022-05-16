/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@JsonTypeName(PassthroughAggregatorFactory.TYPE)
@EverythingIsNonnullByDefault
public class PassthroughAggregatorFactory extends AggregatorFactory
{
  private static final int ESTIMATED_HEAP_FOOTPRINT = 800;

  static final String TYPE = "passthrough";

  private final String columnName;
  private final String complexTypeName;

  @JsonCreator
  public PassthroughAggregatorFactory(
      @JsonProperty("columnName") String columnName,
      @JsonProperty("complexTypeName") String complexTypeName
  )
  {
    this.columnName = columnName;
    this.complexTypeName = complexTypeName;
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @Override
  @JsonProperty
  public String getComplexTypeName()
  {
    return complexTypeName;
  }

  @Override
  public byte[] getCacheKey()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new PassthroughAggregator(metricFactory.makeColumnValueSelector(columnName));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return this;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object deserialize(Object object)
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName()
  {
    return columnName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(columnName);
  }

  @Override
  public int guessAggregatorHeapFootprint(long rows)
  {
    return ESTIMATED_HEAP_FOOTPRINT;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new PassthroughAggregatorFactory(newName, complexTypeName);
  }

  @Override
  public int getMaxIntermediateSize()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.ofComplex(complexTypeName);
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.ofComplex(complexTypeName);
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
    PassthroughAggregatorFactory that = (PassthroughAggregatorFactory) o;
    return columnName.equals(that.columnName) && complexTypeName.equals(that.complexTypeName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, complexTypeName);
  }

  @Override
  public String toString()
  {
    return "PassthroughAggregatorFactory{" +
           "columnName='" + columnName + '\'' +
           ", complexTypeName='" + complexTypeName + '\'' +
           '}';
  }
}
