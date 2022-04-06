/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionDictionary;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedIterable;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Used by {@link NestedDataColumnIndexer} to build the global value dictionary, which can be converted into a
 * {@link GlobalDictionarySortedCollector} to sort and write out the values to a segment with
 * {@link #getSortedCollector()}.
 */
public class GlobalDimensionDictionary
{
  private final ComparatorDimensionDictionary<String> stringDictionary;
  private final ComparatorDimensionDictionary<Long> longDictionary;
  private final ComparatorDimensionDictionary<Double> doubleDictionary;

  public GlobalDimensionDictionary()
  {
    this.stringDictionary = new ComparatorDimensionDictionary<>(ColumnType.STRING.getNullableStrategy());
    this.longDictionary = new ComparatorDimensionDictionary<>(ColumnType.LONG.getNullableStrategy());
    this.doubleDictionary = new ComparatorDimensionDictionary<>(ColumnType.DOUBLE.getNullableStrategy());
  }

  public void addLongValue(@Nullable Long value)
  {
    longDictionary.add(value);
  }

  public void addDoubleValue(@Nullable Double value)
  {
    doubleDictionary.add(value);
  }

  public void addStringValue(@Nullable String value)
  {
    stringDictionary.add(value);
  }

  public GlobalDictionarySortedCollector getSortedCollector()
  {
    final ComparatorSortedDimensionDictionary<String> sortedStringDimensionDictionary =
        stringDictionary.sort();

    Indexed<String> strings = new Indexed<String>()
    {
      @Override
      public int size()
      {
        return stringDictionary.size();
      }

      @Override
      public String get(int index)
      {
        return sortedStringDimensionDictionary.getValueFromSortedId(index);
      }

      @Override
      public int indexOf(String value)
      {
        int id = stringDictionary.getId(value);
        return id < 0
               ? DimensionDictionary.ABSENT_VALUE_ID
               : sortedStringDimensionDictionary.getSortedIdFromUnsortedId(id);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    };

    final ComparatorSortedDimensionDictionary<Long> sortedLongDimensionDictionary =
        longDictionary.sort();

    Indexed<Long> longs = new Indexed<Long>()
    {
      @Override
      public int size()
      {
        return longDictionary.size();
      }

      @Override
      public Long get(int index)
      {
        return sortedLongDimensionDictionary.getValueFromSortedId(index);
      }

      @Override
      public int indexOf(Long value)
      {
        int id = longDictionary.getId(value);
        return id < 0
               ? DimensionDictionary.ABSENT_VALUE_ID
               : sortedLongDimensionDictionary.getSortedIdFromUnsortedId(id);
      }

      @Override
      public Iterator<Long> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    };

    final ComparatorSortedDimensionDictionary<Double> sortedDoubleDimensionDictionary =
        doubleDictionary.sort();

    Indexed<Double> doubles = new Indexed<Double>()
    {
      @Override
      public int size()
      {
        return doubleDictionary.size();
      }

      @Override
      public Double get(int index)
      {
        return sortedDoubleDimensionDictionary.getValueFromSortedId(index);
      }

      @Override
      public int indexOf(Double value)
      {
        int id = doubleDictionary.getId(value);
        return id < 0 ? DimensionDictionary.ABSENT_VALUE_ID : sortedDoubleDimensionDictionary.getSortedIdFromUnsortedId(id);
      }

      @Override
      public Iterator<Double> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    };
    return new GlobalDictionarySortedCollector(strings, longs, doubles);
  }
}
