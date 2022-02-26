/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionary;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class NestedDataColumnIndexer implements DimensionIndexer<StructuredData, StructuredData, StructuredData>
{
  protected volatile boolean hasNulls = false;

  protected SortedMap<String, FieldIndexer<?>> fieldIndexers = new TreeMap<>();
  protected final DimensionDictionary<String> dimLookup = new DimensionDictionary<>(String.class);

  protected final StructuredDataProcessor indexerProcessor = new StructuredDataProcessor()
  {
    @Override
    public void processLiteralField(String fieldName, Object fieldValue)
    {
      FieldIndexer<?> indexer = fieldIndexers.computeIfAbsent(
          fieldName,
          (field) -> new StringFieldIndexer(dimLookup)
      );

      indexer.processValue(fieldValue);
    }
  };
  
  @Override
  public EncodedKeyComponent<StructuredData> processRowValsToUnsortedEncodedKeyComponent(
      @Nullable Object dimValues,
      boolean reportParseExceptions
  )
  {
    final StructuredData data;
    if (dimValues == null) {
      hasNulls = true;
      data = null;
    } else if (dimValues instanceof StructuredData) {
      data = (StructuredData) dimValues;
    } else {
      data = new StructuredData(dimValues);
    }
    indexerProcessor.processFields(dimValues);
    return new EncodedKeyComponent<>(data, data == null ? 0 : data.estimateSize());
  }

  @Override
  public void setSparseIndexed()
  {
    this.hasNulls = true;
  }

  @Override
  public StructuredData getUnsortedEncodedValueFromSorted(StructuredData sortedIntermediateValue)
  {
    return sortedIntermediateValue;
  }

  @Override
  public CloseableIndexed<StructuredData> getSortedIndexedValues()
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public StructuredData getMinValue()
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public StructuredData getMaxValue()
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public int getCardinality()
  {
    return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec spec,
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    return new ObjectColumnSelector<StructuredData>()
    {
      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Nullable
      @Override
      public StructuredData getObject()
      {
        return (StructuredData) currEntry.get().getDims()[dimIndex];
      }

      @Override
      public Class<StructuredData> classOfObject()
      {
        return StructuredData.class;
      }
    };
  }

  @Override
  public ColumnCapabilities getColumnCapabilities()
  {
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(NestedDataComplexTypeSerde.TYPE)
                                 .setHasNulls(hasNulls);
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(
      @Nullable StructuredData lhs,
      @Nullable StructuredData rhs
  )
  {
    return StructuredData.COMPARATOR.compare(lhs, rhs);
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(
      @Nullable StructuredData lhs,
      @Nullable StructuredData rhs
  )
  {
    return Objects.equals(lhs, rhs);
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(@Nullable StructuredData key)
  {
    return Objects.hash(key);
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualList(StructuredData key)
  {
    return key;
  }

  @Override
  public ColumnValueSelector convertUnsortedValuesToSorted(ColumnValueSelector selectorWithUnsortedValues)
  {
    return selectorWithUnsortedValues;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      StructuredData key,
      int rowNum,
      MutableBitmap[] bitmapIndexes,
      BitmapFactory factory
  )
  {
    throw new UnsupportedOperationException("Not supported");
  }

  interface FieldIndexer<T>
  {
    void processValue(@Nullable Object value);
  }

  static class StringFieldIndexer implements FieldIndexer<String>
  {
    private final DimensionDictionary<String> sharedDictionary;

    StringFieldIndexer(DimensionDictionary<String> sharedDictionary)
    {
      this.sharedDictionary = sharedDictionary;
    }

    @Override
    public void processValue(@Nullable Object value)
    {
      if (value == null) {
        sharedDictionary.add(null);
      } else if (value instanceof String) {
        sharedDictionary.add((String) value);
      } else {
        sharedDictionary.add(String.valueOf(value));
      }
    }
  }
}
