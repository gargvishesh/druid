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
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DictionaryEncodedColumnIndexer;
import org.apache.druid.segment.DimensionDictionary;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRow;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.Objects;

public class IpAddressDictionaryEncodedColumnIndexer extends DictionaryEncodedColumnIndexer<Integer, IpAddressBlob>
{
  private final boolean hasBitmapIndexes;
  private final boolean useMaxMemoryEstimates;

  public IpAddressDictionaryEncodedColumnIndexer(boolean hasBitmapIndexes, boolean useMaxMemoryEstimates)
  {
    this.hasBitmapIndexes = hasBitmapIndexes;
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;
  }

  @Nullable
  @Override
  public EncodedKeyComponent<Integer> processRowValsToUnsortedEncodedKeyComponent(
      @Nullable Object dimValues,
      boolean reportParseExceptions
  )
  {
    final long oldDictSizeInBytes = useMaxMemoryEstimates ? 0 : dimLookup.sizeInBytes();
    final int id = dimLookup.add(IpAddressBlob.parse(dimValues, reportParseExceptions));
    final long estimate = useMaxMemoryEstimates
                          ? Integer.BYTES
                          : Integer.BYTES + dimLookup.sizeInBytes() - oldDictSizeInBytes;
    return new EncodedKeyComponent<>(id, estimate);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec spec,
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final ExtractionFn extractionFn = spec.getExtractionFn();

    final int dimIndex = desc.getIndex();

    // maxId is used in concert with getLastRowIndex() in IncrementalIndex to ensure that callers do not encounter
    // rows that contain IDs over the initially-reported cardinality. The main idea is that IncrementalIndex establishes
    // a watermark at the time a cursor is created, and doesn't allow the cursor to walk past that watermark.
    //
    // Additionally, this selector explicitly blocks knowledge of IDs past maxId that may occur from other causes
    // (for example: nulls getting generated for empty arrays, or calls to lookupId).
    final int maxId = getCardinality();

    class IndexerDimensionSelector implements DimensionSelector, IdLookup
    {
      private final ArrayBasedIndexedInts indexedInts = new ArrayBasedIndexedInts();

      @Nullable
      @MonotonicNonNull
      private int[] nullIdIntArray;

      @Override
      public IndexedInts getRow()
      {
        final Object[] dims = currEntry.get().getDims();

        Integer index;
        if (dimIndex < dims.length) {
          index = (Integer) dims[dimIndex];
        } else {
          index = null;
        }

        int[] row = null;
        int rowSize = 0;

        // usually due to currEntry's rowIndex is smaller than the row's rowIndex in which this dim first appears
        if (index == null) {

          final int nullId = getEncodedValue(null, false);
          if (nullId >= 0 && nullId < maxId) {
            // null was added to the dictionary before this selector was created; return its ID.
            if (nullIdIntArray == null) {
              nullIdIntArray = new int[]{nullId};
            }
            row = nullIdIntArray;
            rowSize = 1;
          } else {
            // null doesn't exist in the dictionary; return an empty array.
            // Choose to use ArrayBasedIndexedInts later, instead of special "empty" IndexedInts, for monomorphism
            row = IntArrays.EMPTY_ARRAY;
            rowSize = 0;
          }
        }

        if (row == null && index != null) {
          row = new int[]{index};
          rowSize = 1;
        }

        indexedInts.setValues(row, rowSize);
        return indexedInts;
      }

      @Override
      public ValueMatcher makeValueMatcher(final String value)
      {
        if (extractionFn == null) {
          final int valueId = lookupId(value);
          if (valueId >= 0 || value == null) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                Object[] dims = currEntry.get().getDims();
                if (dimIndex >= dims.length) {
                  return value == null;
                }

                Integer dimsInt = (Integer) dims[dimIndex];
                if (dimsInt == null) {
                  return value == null;
                }

                if (dimsInt == valueId) {
                  return true;
                }
                return false;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                // nothing to inspect
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
        final BitSet checkedIds = new BitSet(maxId);
        final BitSet matchingIds = new BitSet(maxId);
        final boolean matchNull = predicate.apply(null);

        // Lazy matcher; only check an id if matches() is called.
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            Object[] dims = currEntry.get().getDims();
            if (dimIndex >= dims.length) {
              return matchNull;
            }

            Integer dimsInt = (Integer) dims[dimIndex];
            if (dimsInt == null) {
              return matchNull;
            }

            if (checkedIds.get(dimsInt)) {
              if (matchingIds.get(dimsInt)) {
                return true;
              }
            } else {
              final boolean matches = predicate.apply(lookupName(dimsInt));
              checkedIds.set(dimsInt);
              if (matches) {
                matchingIds.set(dimsInt);
                return true;
              }
            }
            return false;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            // nothing to inspect
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        return maxId;
      }

      @Override
      public String lookupName(int id)
      {
        if (id >= maxId) {
          // Sanity check; IDs beyond maxId should not be known to callers. (See comment above.)
          throw new ISE("id[%d] >= maxId[%d]", id, maxId);
        }
        final IpAddressBlob strValue = getActualValue(id, false);
        return extractionFn == null ? strValue.asCompressedString() : extractionFn.apply(strValue);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return dictionaryEncodesAllValues();
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
        if (extractionFn != null) {
          throw new UnsupportedOperationException(
              "cannot perform lookup when applying an extraction function"
          );
        }

        final IpAddressBlob blob = IpAddressBlob.ofString(name);
        final int id = getEncodedValue(blob, false);

        if (id < maxId) {
          return id;
        } else {
          // Can happen if a value was added to our dimLookup after this selector was created. Act like it
          // doesn't exist.
          return DimensionDictionary.ABSENT_VALUE_ID;
        }
      }

      @SuppressWarnings("deprecation")
      @Nullable
      @Override
      public Object getObject()
      {
        IncrementalIndexRow key = currEntry.get();
        if (key == null) {
          return null;
        }

        Object[] dims = key.getDims();
        if (dimIndex >= dims.length) {
          return null;
        }

        return convertUnsortedEncodedKeyComponentToActualList((Integer) dims[dimIndex]);
      }

      @SuppressWarnings("deprecation")
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    }
    return new IndexerDimensionSelector();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities()
  {
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(IpAddressModule.TYPE)
                                 .setDictionaryEncoded(dictionaryEncodesAllValues())
                                 .setHasBitmapIndexes(true)
                                 .setDictionaryValuesUnique(true)
                                 .setDictionaryValuesSorted(false);
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(
      @Nullable Integer lhs,
      @Nullable Integer rhs
  )
  {
    return Comparators.<IpAddressBlob>naturalNullsFirst().compare(
        getActualValue(lhs, false),
        getActualValue(rhs, false)
    );
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(
      @Nullable Integer lhs,
      @Nullable Integer rhs
  )
  {
    return compareUnsortedEncodedKeyComponents(lhs, rhs) == 0;
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(@Nullable Integer key)
  {
    return Objects.hashCode(key);
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualList(Integer key)
  {
    return getActualValue(key, false);
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      Integer key,
      int rowNum,
      MutableBitmap[] bitmapIndexes,
      BitmapFactory factory
  )
  {
    if (!hasBitmapIndexes) {
      throw new UnsupportedOperationException("This column does not include bitmap indexes");
    }

    if (bitmapIndexes[key] == null) {
      bitmapIndexes[key] = factory.makeEmptyMutableBitmap();
    }
    bitmapIndexes[key].add(rowNum);
  }
}
