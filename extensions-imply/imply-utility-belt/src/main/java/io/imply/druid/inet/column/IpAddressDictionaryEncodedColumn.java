/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.imply.druid.inet.IpAddressModule;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

public class IpAddressDictionaryEncodedColumn implements DictionaryEncodedColumn<IpAddressBlob>, ComplexColumn
{
  private final ColumnarInts column;
  private final Indexed<ByteBuffer> dictionary;
  private final Indexed<ImmutableBitmap> bitmaps;

  public IpAddressDictionaryEncodedColumn(
      ColumnarInts column,
      Indexed<ByteBuffer> dictionary,
      Indexed<ImmutableBitmap> bitmaps
  )
  {
    this.column = column;
    this.dictionary = dictionary;
    this.bitmaps = bitmaps;
  }

  @Override
  public int length()
  {
    return column.size();
  }

  @Override
  public boolean hasMultipleValues()
  {
    return false;
  }

  @Override
  public int getSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  @Override
  public IndexedInts getMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException("How can this be?");
  }

  @Nullable
  @Override
  public IpAddressBlob lookupName(int id)
  {
    ByteBuffer blob = dictionary.get(id);
    return IpAddressBlob.ofByteBuffer(blob);
  }

  @Override
  public int lookupId(IpAddressBlob name)
  {
    if (name == null) {
      return dictionary.indexOf(null);
    }
    return dictionary.indexOf(ByteBuffer.wrap(name.getBytes()));
  }

  @Override
  public int getCardinality()
  {
    return dictionary.size();
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      ReadableOffset offset,
      @Nullable ExtractionFn extractionFn
  )
  {

    class IpAddressBlobDimensionSelector extends AbstractDimensionSelector
        implements SingleValueHistoricalDimensionSelector, IdLookup
    {
      private final SingleIndexedInt row = new SingleIndexedInt();

      @Override
      public IndexedInts getRow()
      {
        row.setValue(getRowValue());
        return row;
      }

      public int getRowValue()
      {
        return column.get(offset.getOffset());
      }

      @Override
      public IndexedInts getRow(int offset)
      {
        row.setValue(getRowValue(offset));
        return row;
      }

      @Override
      public int getRowValue(int offset)
      {
        return column.get(offset);
      }

      @Override
      public ValueMatcher makeValueMatcher(final @Nullable String value)
      {
        if (extractionFn == null) {
          final int valueId = lookupId(value);
          if (valueId >= 0) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                return getRowValue() == valueId;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("column", IpAddressDictionaryEncodedColumn.this);
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
        final BitSet checkedIds = new BitSet(getCardinality());
        final BitSet matchingIds = new BitSet(getCardinality());

        // Lazy matcher; only check an id if matches() is called.
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            final int id = getRowValue();

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
            inspector.visit("column", IpAddressDictionaryEncodedColumn.this);
          }
        };
      }

      @Override
      public Object getObject()
      {
        return IpAddressDictionaryEncodedColumn.this.lookupName(getRowValue());
      }

      @Override
      public Class classOfObject()
      {
        return IpAddressBlob.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", column);
        inspector.visit("offset", offset);
        inspector.visit("extractionFn", extractionFn);
      }

      @Override
      public int getValueCardinality()
      {
        /*
         This is technically wrong if
         extractionFn != null && (extractionFn.getExtractionType() != ExtractionFn.ExtractionType.ONE_TO_ONE ||
                                    !extractionFn.preservesOrdering())
         However current behavior allows some GroupBy-V1 queries to work that wouldn't work otherwise and doesn't
         cause any problems due to special handling of extractionFn everywhere.
         See https://github.com/apache/druid/pull/8433
         */
        return getCardinality();
      }

      @Override
      public String lookupName(int id)
      {
        final IpAddressBlob value = IpAddressDictionaryEncodedColumn.this.lookupName(id);
        final String asString = value == null ? null : value.asCompressedString();
        return extractionFn == null ? asString : extractionFn.apply(asString);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        // NameUtf8 is a terrible name if this isn't limited to strings anymore
        return dictionary.get(id);
      }

      @Override
      public boolean supportsLookupNameUtf8()
      {
        return true;
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
        return extractionFn == null ? this : null;
      }

      @Override
      public int lookupId(String name)
      {
        if (extractionFn == null) {
          return IpAddressDictionaryEncodedColumn.this.lookupId(IpAddressBlob.ofString(name));
        }
        throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
      }
    }

    return new IpAddressBlobDimensionSelector();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    // this can't be a normal value selector, index merging expects a dim selector even though it is asking for a value
    // selector, so follow the rules and return one for now
    return makeDimensionSelector(offset, null);
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
  {
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
  {
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    class DictionaryEncodedIpAddressVectorObjectSelector implements VectorObjectSelector
    {
      private final int[] vector = new int[offset.getMaxVectorSize()];
      private final IpAddressBlob[] blobs = new IpAddressBlob[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

      @Override
      public Object[] getObjectVector()
      {
        if (id == offset.getId()) {
          return blobs;
        }

        if (offset.isContiguous()) {
          column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
        }
        for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
          blobs[i] = lookupName(vector[i]);
        }
        id = offset.getId();

        return blobs;
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

    return new DictionaryEncodedIpAddressVectorObjectSelector();
  }

  @Override
  public Class<?> getClazz()
  {
    return IpAddressBlob.class;
  }

  @Override
  public String getTypeName()
  {
    return IpAddressModule.TYPE_NAME;
  }

  @Override
  public Object getRowValue(int rowNum)
  {
    return lookupName(rowNum);
  }

  @Override
  public int getLength()
  {
    return dictionary.size() + bitmaps.size() + column.size();
  }

  @Override
  public void close()
  {
    try {
      column.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
