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
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.LongWrappingDimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@EverythingIsNonnullByDefault
public class OffsetFetchingVirtualColumn implements VirtualColumn
{
  private final String name;

  public OffsetFetchingVirtualColumn(String name)
  {
    this.name = Preconditions.checkNotNull(name, "must have a non-null name");
  }

  @Override
  public byte[] getCacheKey()
  {
    // since the name always stays same, caching would effectively make it a singleton
    return name.getBytes(StandardCharsets.UTF_8);
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
    throw new UnsupportedOperationException();
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
        null
    );
  }

  @Nullable
  @Override
  public SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    return new SingleValueDimensionVectorSelector()
    {
      private final int[] offsets = new int[getMaxVectorSize()];

      @Override
      public int[] getRowVector()
      {
        if (offset.isContiguous()) {
          for (int i = 0; i < getCurrentVectorSize(); i++) {
            offsets[i] = offset.getStartOffset() + i;
          }
          return offsets;
        }
        return offset.getOffsets();
      }

      @Override
      public int getValueCardinality()
      {
        return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
      }

      @Override
      public String lookupName(int id)
      {
        return String.valueOf(id);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return null;
      }

      @Override
      public int getMaxVectorSize()
      {
        return offset.getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return offset.getCurrentVectorSize();
      }
    };
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    return new LongColumnSelector()
    {
      @Override
      public long getLong()
      {
        return offset.getOffset(); // CHEAT :)
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Override
      public boolean isNull()
      {
        return false;
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
    return new VectorValueSelector()
    {
      private final long[] longOffsets = new long[getMaxVectorSize()];

      @Override
      public long[] getLongVector()
      {
        if (offset.isContiguous()) {
          for (int i = 0; i < getCurrentVectorSize(); i++) {
            longOffsets[i] = offset.getStartOffset() + i;
          }
        } else {
          int[] offsets = offset.getOffsets();
          for (int i = 0; i < offsets.length; i++) {
            longOffsets[i] = offsets[i];
          }
        }
        return longOffsets;
      }

      @Override
      public float[] getFloatVector()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public double[] getDoubleVector()
      {
        throw new UnsupportedOperationException();
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getMaxVectorSize()
      {
        return offset.getMaxVectorSize();
      }

      @Override
      public int getCurrentVectorSize()
      {
        return offset.getCurrentVectorSize();
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

  @Override
  public boolean canVectorize(ColumnInspector inspector)
  {
    return true;
  }
}
