/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ObjectStrategy;
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
import java.nio.ByteOrder;
import java.util.BitSet;

public class NestedFieldStringDictionaryEncodedColumn implements DictionaryEncodedColumn<String>
{
  private final ColumnarInts column;
  private final GenericIndexed<String> globalDictionary;
  private final GenericIndexed<Integer> dictionary;

  public NestedFieldStringDictionaryEncodedColumn(
      ColumnarInts column,
      GenericIndexed<String> globalDictionary,
      GenericIndexed<Integer> dictionary
  )
  {
    this.column = column;
    this.globalDictionary = globalDictionary;
    this.dictionary = dictionary;
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
    throw new IllegalStateException("Multi-value row not supported");
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    return globalDictionary.get(dictionary.get(id));
  }

  @Override
  public int lookupId(String name)
  {
    return dictionary.indexOf(globalDictionary.indexOf(name));
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
    // hi, i'm StringDimensionSelector, you may remember me being implemented in such classics as
    // StringDictionaryEncodedColumn and IpAddressDictionaryEncodedColumn...
    class StringDimensionSelector extends AbstractDimensionSelector
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
                inspector.visit("column", NestedFieldStringDictionaryEncodedColumn.this);
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
            inspector.visit("column", NestedFieldStringDictionaryEncodedColumn.this);
          }
        };
      }

      @Override
      public Object getObject()
      {
        return NestedFieldStringDictionaryEncodedColumn.this.lookupName(getRowValue());
      }

      @Override
      public Class classOfObject()
      {
        return String.class;
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
        final String value = NestedFieldStringDictionaryEncodedColumn.this.lookupName(id);
        return extractionFn == null ? value : extractionFn.apply(value);
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
          return NestedFieldStringDictionaryEncodedColumn.this.lookupId(name);
        }
        throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
      }
    }

    return new StringDimensionSelector();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    // this can't be a normal value selector, index merging expects a dim selector even though it is asking for a value
    // selector, so follow the rules and return one for now
    return makeDimensionSelector(offset, null);
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset offset)
  {
    // also copied from StringDictionaryEncodedColumn
    class QueryableSingleValueDimensionVectorSelector implements SingleValueDimensionVectorSelector, IdLookup
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
          column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
        }

        id = offset.getId();
        return vector;
      }

      @Override
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Nullable
      @Override
      public String lookupName(final int id)
      {
        return NestedFieldStringDictionaryEncodedColumn.this.lookupName(id);
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
        return NestedFieldStringDictionaryEncodedColumn.this.lookupId(name);
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

    return new QueryableSingleValueDimensionVectorSelector();
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    // also copied from StringDictionaryEncodedColumn
    class DictionaryEncodedStringSingleValueVectorObjectSelector implements VectorObjectSelector
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
          column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
        }
        for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
          strings[i] = lookupName(vector[i]);
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

    return new DictionaryEncodedStringSingleValueVectorObjectSelector();
  }

  @Override
  public void close() throws IOException
  {
    column.close();
  }

  public static ObjectStrategy<Integer> makeDictionaryStrategy(ByteOrder order)
  {
    final ByteBuffer intConversionbuffer = ByteBuffer.allocate(Integer.BYTES).order(order);
    return new ObjectStrategy<Integer>()
    {
      @Override
      public Class<? extends Integer> getClazz()
      {
        return Integer.class;
      }

      @Nullable
      @Override
      public Integer fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        return buffer.duplicate().order(order).getInt();
      }

      @Nullable
      @Override
      public byte[] toBytes(@Nullable Integer val)
      {
        return intConversionbuffer.putInt(0, val).array();
      }

      @Override
      public int compare(Integer o1, Integer o2)
      {
        return Integer.compare(o1, o2);
      }
    };
  }
}
