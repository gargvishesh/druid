/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.stringmatch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Returns a unique string value if a single value is encountered; otherwise returns null.
 */
@JsonTypeName(StringMatchAggregatorFactory.TYPE_NAME)
public class StringMatchAggregatorFactory extends AggregatorFactory
{
  public static final String TYPE_NAME = "stringMatch";
  public static final ColumnType INTERMEDIATE_TYPE = ColumnType.ofComplex("serializablePairLongString");
  public static final ColumnType FINAL_TYPE = ColumnType.STRING;

  static final int DEFAULT_MAX_STRING_BYTES = 1024;
  private static final byte CACHE_TYPE_ID = (byte) 0xE0;

  private final String fieldName;
  private final String name;
  @Nullable
  private final Integer maxStringBytes;
  private final boolean combine;

  @JsonCreator
  public StringMatchAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("maxStringBytes") final Integer maxStringBytes,
      @JsonProperty("combine") final boolean combine
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    if (maxStringBytes != null && maxStringBytes < 0) {
      throw new IAE("maxStringBytes must be greater than 0");
    }

    this.name = name;
    this.fieldName = fieldName;
    this.maxStringBytes = maxStringBytes;
    this.combine = combine;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    if (combine) {
      final ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(fieldName);
      return new StringMatchCombiningAggregator(selector, getMaxStringBytesOrDefault());
    } else {
      final DimensionSelector selector =
          columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));

      if (selector.getValueCardinality() >= 0 && selector.idLookup() != null) {
        return new StringMatchDictionaryAggregator(selector, getMaxStringBytesOrDefault());
      } else {
        return new StringMatchAggregator(selector, getMaxStringBytesOrDefault());
      }
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    if (combine) {
      final ColumnValueSelector<?> selector = columnSelectorFactory.makeColumnValueSelector(fieldName);
      return new StringMatchCombiningBufferAggregator(selector, getMaxStringBytesOrDefault());
    } else {
      final DimensionSelector selector =
          columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));

      if (selector.getValueCardinality() >= 0 && selector.idLookup() != null) {
        return new StringMatchDictionaryBufferAggregator(selector, getMaxStringBytesOrDefault());
      } else {
        return new StringMatchBufferAggregator(selector, getMaxStringBytesOrDefault());
      }
    }
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    final ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(fieldName);
    final SingleValueDimensionVectorSelector selector;

    if (capabilities == null) {
      selector = NilVectorSelector.create(selectorFactory.getReadableVectorInspector());
    } else {
      // canVectorize ensures that nonnull capabilities => dict-encoded singly-valued string
      selector = selectorFactory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of(fieldName));
    }

    return new StringMatchDictionaryVectorAggregator(selector, getMaxStringBytesOrDefault());
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    if (combine) {
      return false;
    }

    final ColumnCapabilities capabilities = columnInspector.getColumnCapabilities(fieldName);
    return capabilities == null || (capabilities.is(ValueType.STRING)
                                    && capabilities.hasMultipleValues().isFalse()
                                    && capabilities.isDictionaryEncoded().isTrue());
  }

  @Override
  public Comparator getComparator()
  {
    return Comparators.naturalNullsFirst();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    final SerializablePairLongString lhsPair = (SerializablePairLongString) lhs;
    final SerializablePairLongString rhsPair = (SerializablePairLongString) rhs;

    if (lhsPair.lhs == StringMatchUtil.MATCHING && rhsPair.lhs == StringMatchUtil.MATCHING) {
      if (rhsPair.rhs == null) {
        return lhsPair;
      } else if (lhsPair.rhs == null) {
        return rhsPair;
      } else if (lhsPair.rhs.equals(rhsPair.rhs)) {
        return lhsPair;
      }
    }

    return new SerializablePairLongString((long) StringMatchUtil.NOT_MATCHING, null);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringMatchAggregatorFactory(name, name, maxStringBytes, true);
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    return new SerializablePairLongString(((Number) map.get("lhs")).longValue(), ((String) map.get("rhs")));
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object == null ? null : ((SerializablePairLongString) object).rhs;
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

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getMaxStringBytes()
  {
    return maxStringBytes;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isCombine()
  {
    return combine;
  }

  public int getMaxStringBytesOrDefault()
  {
    return maxStringBytes != null ? maxStringBytes : DEFAULT_MAX_STRING_BYTES;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendInt(getMaxStringBytesOrDefault())
        .appendBoolean(combine)
        .build();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return INTERMEDIATE_TYPE;
  }

  @Override
  public ColumnType getResultType()
  {
    return FINAL_TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    // matching? + string length + string
    return Byte.BYTES + Integer.BYTES + getMaxStringBytesOrDefault();
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new StringMatchAggregatorFactory(newName, fieldName, maxStringBytes, combine);
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
    StringMatchAggregatorFactory that = (StringMatchAggregatorFactory) o;
    return combine == that.combine
           && Objects.equals(fieldName, that.fieldName)
           && Objects.equals(name, that.name)
           && Objects.equals(maxStringBytes, that.maxStringBytes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name, maxStringBytes, combine);
  }

  @Override
  public String toString()
  {
    return "StringMatchAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           (maxStringBytes == null ? "" : ", maxStringBytes=" + maxStringBytes) +
           (combine ? "combine=true" : "") +
           '}';
  }
}
