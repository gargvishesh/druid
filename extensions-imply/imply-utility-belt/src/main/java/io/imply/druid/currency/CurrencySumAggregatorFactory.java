/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.currency;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CurrencySumAggregatorFactory extends AggregatorFactory
{
  public static final String TYPE_NAME = "currencySum";

  private static final byte CACHE_TYPE_ID = 22;
  private static final Comparator COMPARATOR = new Ordering()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Doubles.compare(((Number) o).doubleValue(), ((Number) o1).doubleValue());
    }
  }.nullsFirst();

  private final String name;
  private final String fieldName;
  private final TreeMap<Long, Double> conversions;
  private final boolean storeDoubleAsFloat;

  @JsonCreator
  public CurrencySumAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("conversions") final Map<DateTime, Double> conversions
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
    this.conversions = buildConversionsMap(conversions);
    this.storeDoubleAsFloat = ColumnHolder.storeDoubleAsFloat();
  }

  private static TreeMap<Long, Double> buildConversionsMap(final Map<DateTime, Double> conversions)
  {
    if (conversions == null) {
      return new TreeMap<>();
    }

    final TreeMap<Long, Double> retVal = new TreeMap<>();
    for (Map.Entry<DateTime, Double> entry : conversions.entrySet()) {
      retVal.put(entry.getKey().getMillis(), entry.getValue());
    }

    return retVal;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public Map<DateTime, Double> getConversions()
  {
    final Map<DateTime, Double> conversionsWithDateTimeKeys = new TreeMap<>();
    for (Map.Entry<Long, Double> entry : conversions.entrySet()) {
      conversionsWithDateTimeKeys.put(new DateTime(entry.getKey(), DateTimeZone.UTC), entry.getValue());
    }
    return conversionsWithDateTimeKeys;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    return new CurrencySumAggregator(
        factorizeBuffered(columnSelectorFactory),
        ByteBuffer.allocate(getMaxIntermediateSize()).order(ByteOrder.nativeOrder())
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    return new CurrencySumBufferAggregator(
        conversions,
        columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME),
        columnSelectorFactory.makeColumnValueSelector(fieldName)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleSumAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return ImmutableList.of(
        new DoubleSumAggregatorFactory(fieldName, fieldName)
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Double.parseDouble((String) object);
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of(ColumnHolder.TIME_COLUMN_NAME, fieldName);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    if (storeDoubleAsFloat) {
      return ColumnType.FLOAT;
    }
    return ColumnType.DOUBLE;
  }

  @Override
  public ColumnType getResultType()
  {
    return getIntermediateType();
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    final ByteBuffer cacheKey = ByteBuffer
        .allocate(1 + Ints.BYTES + fieldNameBytes.length + conversions.size() * (Longs.BYTES + Doubles.BYTES))
        .put(CACHE_TYPE_ID)
        .putInt(fieldNameBytes.length)
        .put(fieldNameBytes);

    for (Map.Entry<Long, Double> conversion : conversions.entrySet()) {
      cacheKey.putLong(conversion.getKey());
      cacheKey.putDouble(conversion.getValue());
    }

    return cacheKey.array();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES + 2 * Doubles.BYTES;
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

    CurrencySumAggregatorFactory that = (CurrencySumAggregatorFactory) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    return conversions != null ? conversions.equals(that.conversions) : that.conversions == null;

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + (conversions != null ? conversions.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "CurrencySumAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", conversions=" + conversions +
           '}';
  }
}
