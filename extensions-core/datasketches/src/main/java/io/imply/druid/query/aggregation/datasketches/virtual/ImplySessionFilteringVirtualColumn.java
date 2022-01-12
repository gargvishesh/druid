/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.virtual;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.druid.query.aggregation.ImplyAggregationUtil;
import org.apache.datasketches.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ImplySessionFilteringVirtualColumn implements VirtualColumn
{
  private final String name;
  private final String field;
  private final AtomicReference<Set<Long>> filterValues;

  @JsonCreator
  public ImplySessionFilteringVirtualColumn(@JsonProperty("name") String name, @JsonProperty("field") String field)
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.field = Preconditions.checkNotNull(field, "field");
    this.filterValues = new AtomicReference<>();
  }


  @JsonProperty("name")
  @Override
  public String getOutputName()
  {
    return name;
  }

  @JsonProperty
  public String getField()
  {
    return field;
  }

  public AtomicReference<Set<Long>> getFilterValues()
  {
    return filterValues;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(ImplyAggregationUtil.SESSION_FILTERING_VIRTUAL_COLUMN_CACHE_ID)
        .appendString(field)
        .build();
  }


  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    throw new UOE("Cannot make a dimension selector");
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    throw new UOE("Cannot make a dimension selector");
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                       .setDictionaryEncoded(true)
                                       .setHasBitmapIndexes(true);
  }

  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    return new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                       .setDictionaryEncoded(true)
                                       .setHasBitmapIndexes(true);
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.emptyList();
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public @Nullable BitmapIndex getBitmapIndex(
      String columnName,
      ColumnSelector selector
  )
  {
    final ColumnHolder holder = selector.getColumnHolder(field);
    if (holder == null) {
      return null;
    }
    final BitmapIndex underlyingIndex = holder.getBitmapIndex();
    if (underlyingIndex == null) {
      return null;
    }
    return new ImplyListFilteredBitmapIndex(underlyingIndex);
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
    ImplySessionFilteringVirtualColumn that = (ImplySessionFilteringVirtualColumn) o;
    return name.equals(that.name) && field.equals(that.field) && Objects.equals(filterValues.get(), that.filterValues.get());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, filterValues);
  }

  @Override
  public String toString()
  {
    return "ImplySessionFilteringVirtualColumn{" +
           "name='" + name + '\'' +
           ", field='" + field + '\'' +
           ", values=" + filterValues +
           '}';
  }

  private class ImplyListFilteredBitmapIndex implements BitmapIndex
  {
    final BitmapIndex delegate;

    private ImplyListFilteredBitmapIndex(BitmapIndex delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public String getValue(int index)
    {
      return delegate.getValue(index);
    }

    @Override
    public boolean hasNulls()
    {
      return delegate.hasNulls();
    }

    @Override
    public BitmapFactory getBitmapFactory()
    {
      return delegate.getBitmapFactory();
    }

    @Override
    public ImmutableBitmap getBitmap(int idx)
    {
      Set<Long> theVals = filterValues.get();
      if (theVals == null) {
        return delegate.getBitmapFactory().makeEmptyImmutableBitmap();
      }
      if (idx == 1) {
        BitmapFactory bitmapFactory = delegate.getBitmapFactory();
        List<ImmutableBitmap> bitmaps = new ArrayList<>(100);
        for (int i = 0; i < delegate.getCardinality(); i++) {
          String dimVal = delegate.getValue(i);
          if (dimVal != null &&
              !dimVal.isEmpty() &&
              theVals.contains(
                  MurmurHash3.hash(dimVal.getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0] >>> 1
              )
          ) {
            bitmaps.add(delegate.getBitmap(i));
            if (bitmaps.size() >= 100) {
              ImmutableBitmap partialUnion = bitmapFactory.union(bitmaps);
              bitmaps.clear();
              bitmaps.add(partialUnion);
            }
          }
        }
        return bitmapFactory.union(bitmaps);
      }
      throw new UOE("Unexpected index %d for bitmap lookup", idx);
    }

    @Override
    public int getCardinality()
    {
      return 1;
    }

    @Override
    public int getIndex(@Nullable String value)
    {
      if (value == null) {
        throw new UnsupportedOperationException("Session filtering doesn't support filtering with null");
      }
      if (filterValues.get() == null) {
        byte[] hashBytes = StringUtils.decodeBase64String(value);
        long[] hashes = new long[hashBytes.length / Long.BYTES];
        ByteBuffer byteBuffer = ByteBuffer.allocate(hashBytes.length).put(hashBytes);
        byteBuffer.position(0);
        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        longBuffer.get(hashes);
        filterValues.set(Arrays.stream(hashes).boxed().collect(Collectors.toSet()));
      }
      return 1;
    }
  }
}
