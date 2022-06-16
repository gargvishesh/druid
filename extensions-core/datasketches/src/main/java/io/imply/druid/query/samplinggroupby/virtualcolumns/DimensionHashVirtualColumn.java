/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.virtualcolumns;

import com.google.common.base.Preconditions;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.LongWrappingDimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@EverythingIsNonnullByDefault
public class DimensionHashVirtualColumn implements VirtualColumn
{
  private final Memory hashMemory;
  private final String name;
  private int index;

  public DimensionHashVirtualColumn(Memory hashMemory, String name)
  {
    this.hashMemory = Preconditions.checkNotNull(hashMemory, "hashMemory is null");
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.index = 0;
  }

  @Override
  public byte[] getCacheKey()
  {
    return UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8); // since this is internal column, no point in caching
  }

  @Override
  public String getOutputName()
  {
    return name;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    return new LongWrappingDimensionSelector(
      Objects.requireNonNull(makeColumnValueSelector(dimensionSpec.getDimension(), factory)),
      null);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    return new LongWrappingDimensionSelector(
        Objects.requireNonNull(makeColumnValueSelector(dimensionSpec.getDimension(), columnSelector, offset)),
        null);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    return new ColumnValueSelector<Long>() {

      @Nullable
      @Override
      public Long getObject()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Class<? extends Long> classOfObject()
      {
        return Long.class;
      }

      @Override
      public boolean isNull()
      {
        return false;
      }

      @Override
      public long getLong()
      {
        return hashMemory.getLong((long) index * Long.BYTES);
      }

      @Override
      public float getFloat()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public double getDouble()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    };
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      final ReadableOffset offset
  )
  {
    return new ColumnValueSelector<Long>() {

      @Nullable
      @Override
      public Long getObject()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Class<? extends Long> classOfObject()
      {
        return Long.class;
      }

      @Override
      public boolean isNull()
      {
        return false;
      }

      @Override
      public long getLong()
      {
        return hashMemory.getLong((long) offset.getOffset() * Long.BYTES);
      }

      @Override
      public float getFloat()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public double getDouble()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    };
  }

  @Nullable
  @Override
  public VectorValueSelector makeVectorValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    return new BaseLongVectorValueSelector(offset)
    {
      @Nullable private long[] hashesVector;

      @Override
      public long[] getLongVector()
      {
        if (hashesVector == null) {
          hashesVector = new long[offset.getMaxVectorSize()];
        }
        if (offset.isContiguous()) {
          for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
            hashesVector[i] = hashMemory.getLong((long) (offset.getStartOffset() + i) * Long.BYTES);
          }
          return hashesVector;
        }
        int[] offsets = offset.getOffsets();
        for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
          hashesVector[i] = hashMemory.getLong((long) offsets[i] * Long.BYTES);
        }
        return hashesVector;
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        return null;
      }
    };
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
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

  public void setIndex(int index)
  {
    this.index = index;
  }

  @Override
  public boolean canVectorize(ColumnInspector inspector)
  {
    return true;
  }
}
