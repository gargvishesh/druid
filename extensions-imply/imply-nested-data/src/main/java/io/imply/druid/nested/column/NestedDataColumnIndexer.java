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
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
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

  protected SortedMap<String, LiteralFieldIndexer> fieldIndexers = new TreeMap<>();
  protected final GlobalDimensionDictionary globalDictionary = new GlobalDimensionDictionary();

  int estimatedFieldKeySize = 0;

  protected final StructuredDataProcessor indexerProcessor = new StructuredDataProcessor()
  {
    @Override
    public int processLiteralField(String fieldName, Object fieldValue)
    {
      LiteralFieldIndexer fieldIndexer = fieldIndexers.get(fieldName);
      if (fieldIndexer == null) {
        estimatedFieldKeySize += StructuredDataProcessor.estimateStringSize(fieldName);
        fieldIndexer = new LiteralFieldIndexer(globalDictionary);
        fieldIndexers.put(fieldName, fieldIndexer);
      }
      return fieldIndexer.processValue(fieldValue);
    }
  };
  
  @Override
  public EncodedKeyComponent<StructuredData> processRowValsToUnsortedEncodedKeyComponent(
      @Nullable Object dimValues,
      boolean reportParseExceptions
  )
  {
    final long oldDictSizeInBytes = globalDictionary.sizeInBytes();
    final int oldFieldKeySize = estimatedFieldKeySize;
    final StructuredData data;
    if (dimValues == null) {
      hasNulls = true;
      data = null;
    } else if (dimValues instanceof StructuredData) {
      data = (StructuredData) dimValues;
    } else {
      data = new StructuredData(dimValues);
    }
    StructuredDataProcessor.ProcessResults info = indexerProcessor.processFields(data == null ? null : data.getValue());
    // 'raw' data is currently preserved 'as-is', and not replaced with object references to the global dictionaries
    long effectiveSizeBytes = info.getEstimatedSize();
    // then, we add the delta of size change to the global dictionaries to account for any new space added by the
    // 'raw' data
    effectiveSizeBytes += (globalDictionary.sizeInBytes() - oldDictSizeInBytes);
    effectiveSizeBytes += (estimatedFieldKeySize - oldFieldKeySize);
    return new EncodedKeyComponent<>(data, effectiveSizeBytes);
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
    return globalDictionary.getCardinality();
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

  interface FieldIndexer
  {
    int processValue(@Nullable Object value);
  }

  static class LiteralFieldIndexer implements FieldIndexer
  {
    private final GlobalDimensionDictionary globalDimensionDictionary;
    private final NestedLiteralTypeInfo.MutableTypeSet typeSet;

    LiteralFieldIndexer(GlobalDimensionDictionary globalDimensionDictionary)
    {
      this.globalDimensionDictionary = globalDimensionDictionary;
      this.typeSet = new NestedLiteralTypeInfo.MutableTypeSet();
    }

    @Override
    public int processValue(@Nullable Object value)
    {
      // null value is always added to the global dictionary as id 0, so we can ignore them here
      if (value != null) {
        // why not
        ExprEval<?> eval = ExprEval.bestEffortOf(value);
        final ColumnType columnType = ExpressionType.toColumnType(eval.type());
        typeSet.add(columnType);
        switch (columnType.getType()) {
          case LONG:
            globalDimensionDictionary.addLongValue(eval.asLong());
            return StructuredDataProcessor.getLongObjectEstimateSize();
          case DOUBLE:
            globalDimensionDictionary.addDoubleValue(eval.asDouble());
            return StructuredDataProcessor.getDoubleObjectEstimateSize();
          case STRING:
          default:
            final String asString = eval.asString();
            globalDimensionDictionary.addStringValue(asString);
            return StructuredDataProcessor.estimateStringSize(asString);
        }
      }
      return 0;
    }

    public NestedLiteralTypeInfo.MutableTypeSet getTypes()
    {
      return typeSet;
    }
  }
}
