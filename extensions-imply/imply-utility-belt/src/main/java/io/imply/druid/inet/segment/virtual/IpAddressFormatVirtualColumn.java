/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.segment.virtual;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.imply.druid.inet.column.IpAddressBitmapIndex;
import io.imply.druid.inet.column.IpAddressBlob;
import io.imply.druid.inet.column.IpAddressDictionaryEncodedColumn;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.historical.HistoricalDimensionSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IpAddressFormatVirtualColumn implements VirtualColumn
{
  public static final String TYPE_NAME = "ip-format";

  private final String name;
  private final String field;
  private final boolean compact;
  private final boolean forceV6;

  @JsonCreator
  public IpAddressFormatVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("field") String field,
      @JsonProperty("compact") @Nullable Boolean compactFormat,
      @JsonProperty("forceV6") @Nullable Boolean forceV6
  )
  {
    this.name = Preconditions.checkNotNull(name, "name must be specified");
    this.field = Preconditions.checkNotNull(field, "field must be specified");
    this.compact = compactFormat == null || compactFormat;
    this.forceV6 = forceV6 != null && forceV6;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

  @JsonProperty("name")
  @Override
  public String getOutputName()
  {
    return name;
  }

  @JsonProperty("field")
  public String getField()
  {
    return field;
  }

  @JsonProperty("compact")
  public boolean isCompact()
  {
    return compact;
  }

  @JsonProperty("forceV6")
  public boolean isForceV6()
  {
    return forceV6;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    final DimensionSelector delegateSelector = dimensionSpec.decorate(factory.makeDimensionSelector(new DefaultDimensionSpec(field, field)));
    class StringifyIpDimensionSelector extends AbstractDimensionSelector
    {

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", delegateSelector);
      }

      @Override
      public Class<?> classOfObject()
      {
        return String.class;
      }

      @Override
      public Object getObject()
      {
        IpAddressBlob blob = (IpAddressBlob) delegateSelector.getObject();
        return blob == null ? null : blob.stringify(compact, forceV6);
      }

      @Override
      public int getValueCardinality()
      {
        return delegateSelector.getValueCardinality();
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        String value = delegateSelector.lookupName(id);
        if (value == null) {
          return null;
        } else {
          IpAddressBlob blob = IpAddressBlob.ofString(value);
          if (blob == null) {
            return null;
          } else {
            return blob.stringify(compact, forceV6);
          }
        }
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return delegateSelector.nameLookupPossibleInAdvance();
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return delegateSelector.idLookup();
      }

      @Override
      public IndexedInts getRow()
      {
        return delegateSelector.getRow();
      }

      @Override
      public ValueMatcher makeValueMatcher(final @Nullable String value)
      {
        return delegateSelector.makeValueMatcher(value);
      }

      @Override
      public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
      {
        return delegateSelector.makeValueMatcher(predicate);
      }
    }

    return new StringifyIpDimensionSelector();
  }

  @Nullable
  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    final IpAddressDictionaryEncodedColumn ipAddressColumn = getColumnFromColumnSelector(columnSelector);
    final ColumnarInts encodedValuesColumn = ipAddressColumn.getEncodedValuesColumn();
    final DimensionSelector delegateSelector = ipAddressColumn.makeDimensionSelector(
        offset,
        dimensionSpec.getExtractionFn()
    );

    class StringifyIpDimensionSelector extends AbstractDimensionSelector
        implements HistoricalDimensionSelector, IdLookup
    {

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", ipAddressColumn);
        inspector.visit("selector", delegateSelector);
      }

      @Override
      public Class<?> classOfObject()
      {
        return String.class;
      }

      @Override
      public int getValueCardinality()
      {
        return delegateSelector.getValueCardinality();
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        IpAddressBlob blob = ipAddressColumn.lookupName(id);
        if (blob == null) {
          return null;
        }
        return blob.stringify(compact, forceV6);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return delegateSelector.nameLookupPossibleInAdvance();
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return dimensionSpec.getExtractionFn() == null ? this : null;
      }

      @Override
      public IndexedInts getRow()
      {
        return delegateSelector.getRow();
      }

      @Override
      public ValueMatcher makeValueMatcher(final @Nullable String value)
      {
        if (dimensionSpec.getExtractionFn() == null) {
          final int valueId = lookupId(value);
          if (valueId >= 0) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                return encodedValuesColumn.get(offset.getOffset()) == valueId;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("column", ipAddressColumn);
              }
            };
          } else {
            return BooleanValueMatcher.of(false);
          }
        } else {
          // Employ caching BitSet optimization
          return makeValueMatcher(Predicates.equalTo(value));
        }
      }

      @Override
      public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
      {
        final BitSet checkedIds = new BitSet(ipAddressColumn.getCardinality());
        final BitSet matchingIds = new BitSet(ipAddressColumn.getCardinality());

        // Lazy matcher; only check an id if matches() is called.
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            final int id = encodedValuesColumn.get(offset.getOffset());

            if (checkedIds.get(id)) {
              return matchingIds.get(id);
            } else {
              final boolean matches = predicate.apply(lookupName(id));
              checkedIds.set(id);
              if (matches) {
                matchingIds.set(id);
              }
              return matches;
            }
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("column", ipAddressColumn);
          }
        };
      }

      @Override
      public int lookupId(@Nullable String name)
      {
        IpAddressBlob blob = IpAddressBlob.ofString(name);
        if (blob == null) {
          return 0;
        }
        return ipAddressColumn.lookupId(blob);
      }

      @Override
      public IndexedInts getRow(int offset)
      {
        return delegateSelector.getRow();
      }
    }

    return new StringifyIpDimensionSelector();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    return makeDimensionSelector(DefaultDimensionSpec.of(field), factory);
  }

  @Nullable
  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    return makeDimensionSelector(DefaultDimensionSpec.of(field), columnSelector, offset);
  }

  @Override
  public boolean canVectorize(ColumnInspector inspector)
  {
    return true;
  }


  @Nullable
  @Override
  public SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final IpAddressDictionaryEncodedColumn ipAddressColumn = getColumnFromColumnSelector(columnSelector);
    final ColumnarInts encodedValuesColumn = ipAddressColumn.getEncodedValuesColumn();

    final class StringifyIpSingleValueDimensionVectorSelector implements SingleValueDimensionVectorSelector, IdLookup
    {
      private final int[] vector = new int[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

      @Override
      public int[] getRowVector()
      {
        if (id == offset.getId()) {
          return vector;
        }

        if (offset.isContiguous()) {
          encodedValuesColumn.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          encodedValuesColumn.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
        }

        id = offset.getId();
        return vector;
      }

      @Override
      public int getValueCardinality()
      {
        return ipAddressColumn.getCardinality();
      }

      @Nullable
      @Override
      public String lookupName(final int id)
      {
        IpAddressBlob blob = ipAddressColumn.lookupName(id);
        if (blob == null) {
          return null;
        }
        return blob.stringify(compact, forceV6);
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
        return this;
      }

      @Override
      public int lookupId(@Nullable final String name)
      {
        IpAddressBlob blob = IpAddressBlob.ofString(name);
        if (blob == null) {
          return 0;
        }
        return ipAddressColumn.lookupId(blob);
      }

      @Override
      public int getCurrentVectorSize()
      {
        return offset.getCurrentVectorSize();
      }

      @Override
      public int getMaxVectorSize()
      {
        return offset.getMaxVectorSize();
      }
    }

    return new StringifyIpSingleValueDimensionVectorSelector();
  }

  @Nullable
  @Override
  public VectorObjectSelector makeVectorObjectSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final IpAddressDictionaryEncodedColumn ipAddressColumn = getColumnFromColumnSelector(columnSelector);
    final ColumnarInts encodedValuesColumn = ipAddressColumn.getEncodedValuesColumn();

    final class StringifyIpVectorObjectSelector implements VectorObjectSelector
    {
      private final int[] vector = new int[offset.getMaxVectorSize()];
      private final String[] strings = new String[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

      @Override

      public Object[] getObjectVector()
      {
        if (id == offset.getId()) {
          return strings;
        }

        if (offset.isContiguous()) {
          encodedValuesColumn.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          encodedValuesColumn.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
        }
        final Int2ObjectMap<String> stringifyCache = new Int2ObjectArrayMap<>(offset.getMaxVectorSize());
        for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
          strings[i] = stringifyCache.computeIfAbsent(i, (_i) -> {
            IpAddressBlob blob = ipAddressColumn.lookupName(vector[_i]);
            if (blob == null) {
              return null;
            } else {
              return blob.stringify(compact, forceV6);
            }
          });
        }
        id = offset.getId();

        return strings;
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
    }

    return new StringifyIpVectorObjectSelector();
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                                 .setHasBitmapIndexes(true)
                                 .setDictionaryEncoded(true)
                                 .setDictionaryValuesUnique(true)
                                 .setDictionaryValuesSorted(true)
                                 .setFilterable(true);
  }

  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                                 .setHasBitmapIndexes(true)
                                 .setDictionaryEncoded(true)
                                 .setDictionaryValuesUnique(true)
                                 .setDictionaryValuesSorted(true)
                                 .setFilterable(true);
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.singletonList(field);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Nullable
  @Override
  public BitmapIndex getBitmapIndex(
      String columnName,
      ColumnSelector selector
  )
  {
    final IpAddressDictionaryEncodedColumn ipAddressColumn = getColumnFromColumnSelector(selector);
    // holder definitely exists if we got here
    final ColumnHolder holder = selector.getColumnHolder(field);
    final BitmapIndex delegate = holder.getBitmapIndex();
    if (delegate == null) {
      return null;
    }
    final GenericIndexed<ImmutableBitmap> bitmaps = ipAddressColumn.getBitmaps();
    final GenericIndexed<ByteBuffer> dictionary = ipAddressColumn.getDictionary();

    return new IpAddressBitmapIndex(delegate.getBitmapFactory(), bitmaps, dictionary)
    {
      @Nullable
      @Override
      public String getValue(int index)
      {
        ByteBuffer value = dictionary.get(index);
        if (value == null) {
          return null;
        }
        IpAddressBlob blob = IpAddressBlob.ofByteBuffer(value);
        if (blob == null) {
          return null;
        }
        return blob.stringify(compact, forceV6);
      }
    };
  }

  @Override
  public String toString()
  {
    return "IpAddressFormatVirtualColumn{" +
           "name='" + name + '\'' +
           ", field='" + field + '\'' +
           ", compact=" + compact +
           ", forceV6=" + forceV6 +
           '}';
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
    IpAddressFormatVirtualColumn that = (IpAddressFormatVirtualColumn) o;
    return compact == that.compact && forceV6 == that.forceV6 && name.equals(that.name) && field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, compact, forceV6);
  }

  private IpAddressDictionaryEncodedColumn getColumnFromColumnSelector(ColumnSelector columnSelector)
  {
    final ColumnHolder holder = columnSelector.getColumnHolder(field);
    if (holder == null) {
      throw new IAE("Column [%s] does not exist", field);
    }
    final BaseColumn baseColumn = holder.getColumn();
    if (!(baseColumn instanceof IpAddressDictionaryEncodedColumn)) {
      throw new IAE(
          "Column [%s] is not an instance of [%s]",
          field,
          IpAddressDictionaryEncodedColumn.class.getSimpleName()
      );
    }
    return (IpAddressDictionaryEncodedColumn) holder.getColumn();
  }
}
