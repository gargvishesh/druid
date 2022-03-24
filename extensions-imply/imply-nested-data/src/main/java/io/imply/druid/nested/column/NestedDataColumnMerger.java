/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DictionaryMergingIterator;
import org.apache.druid.segment.DimensionDictionary;
import org.apache.druid.segment.DimensionMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.QueryableIndexIndexableAdapter;
import org.apache.druid.segment.SortedDimensionDictionary;
import org.apache.druid.segment.StringDimensionMergerV9;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedIterable;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.segment.serde.ComplexColumnPartSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.IntBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class NestedDataColumnMerger implements DimensionMergerV9
{
  private static final Logger log = new Logger(NestedDataColumnMerger.class);

  private final String name;
  private final Closer closer;
  @Nullable
  protected List<IndexableAdapter> adapters;

  private NestedDataColumnSerializer serializer;

  public NestedDataColumnMerger(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilites,
      ProgressIndicator progressIndicator,
      Closer closer
  )
  {
    this.name = name;
    this.serializer = new NestedDataColumnSerializer(name, indexSpec, segmentWriteOutMedium, progressIndicator, closer);
    this.closer = closer;
  }

  @Override
  public void writeMergedValueDictionary(List<IndexableAdapter> adapters) throws IOException
  {

    long dimStartTime = System.currentTimeMillis();

    this.adapters = adapters;

    int numMergeIndex = 0;
    Indexed<String> sortedLookup = null;
    final Indexed<String>[] sortedLookups = new Indexed[adapters.size()];

    final SortedSet<String> mergedFields = new TreeSet<>();

    for (int i = 0; i < adapters.size(); i++) {
      final IndexableAdapter adapter = adapters.get(i);
      @SuppressWarnings("MustBeClosedChecker") // we register dimValues in the closer
      final Indexed<String> dimValues;
      if (adapter instanceof IncrementalIndexAdapter) {
        dimValues = getSortedIndexFromIncrementalAdapter((IncrementalIndexAdapter) adapter, mergedFields);
      } else if (adapter instanceof QueryableIndexIndexableAdapter) {
        dimValues = getSortedIndexFromQueryableAdapter((QueryableIndexIndexableAdapter) adapter, mergedFields);
      } else {
        dimValues = null;
      }

      if (dimValues != null && !allNull(dimValues)) {
        sortedLookups[i] = sortedLookup = dimValues;
        numMergeIndex++;
      }
    }

    serializer.open();
    serializer.serializeFields(mergedFields);

    int cardinality = 0;
    if (numMergeIndex > 1) {
      DictionaryMergingIterator<String> dictionaryMergeIterator = new DictionaryMergingIterator<>(
          sortedLookups,
          StringDimensionMergerV9.DICTIONARY_MERGING_COMPARATOR,
          true
      );
      serializer.serializeDictionary(() -> dictionaryMergeIterator);
      cardinality = dictionaryMergeIterator.getCounter();
    } else if (numMergeIndex == 1) {
      serializer.serializeDictionary(sortedLookup);
      cardinality = sortedLookup.size();
    }

    log.debug(
        "Completed dim[%s] conversions with cardinality[%,d] in %,d millis.",
        name,
        cardinality,
        System.currentTimeMillis() - dimStartTime
    );
  }

  @Nullable
  private Indexed<String> getSortedIndexFromIncrementalAdapter(
      IncrementalIndexAdapter adapter,
      SortedSet<String> mergedFields
  )
  {
    final IncrementalIndex index = adapter.getIndex();
    final IncrementalIndex.DimensionDesc dim = index.getDimension(name);
    if (dim == null || !(dim.getIndexer() instanceof NestedDataColumnIndexer)) {
      return null;
    }
    final NestedDataColumnIndexer indexer = (NestedDataColumnIndexer) dim.getIndexer();
    mergedFields.addAll(indexer.fieldIndexers.keySet());
    final SortedDimensionDictionary<String> dimensionDictionary = indexer.dimLookup.sort();
    return closer.register(new CloseableIndexed<String>()
    {
      @Override
      public int size()
      {
        return indexer.dimLookup.size();
      }

      @Override
      public String get(int index)
      {
        return dimensionDictionary.getValueFromSortedId(index);
      }

      @Override
      public int indexOf(String value)
      {
        int id = indexer.dimLookup.getId(value);
        return id < 0 ? DimensionDictionary.ABSENT_VALUE_ID : dimensionDictionary.getSortedIdFromUnsortedId(id);
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

      @Override
      public void close()
      {
        // nothing to close
      }
    });
  }

  @Nullable
  private Indexed<String> getSortedIndexFromQueryableAdapter(
      QueryableIndexIndexableAdapter adapter,
      SortedSet<String> mergedFields
  )
  {
    final ColumnHolder columnHolder = adapter.getQueryableIndex().getColumnHolder(name);

    if (columnHolder == null) {
      return null;
    }

    final BaseColumn col = columnHolder.getColumn();

    if (!(col instanceof NestedDataComplexColumn)) {
      return null;
    }

    @SuppressWarnings("unchecked")
    NestedDataComplexColumn column = (NestedDataComplexColumn) col;

    for (String s : column.fields) {
      mergedFields.add(s);
    }

    return closer.register(new CloseableIndexed<String>()
    {
      @Override
      public int size()
      {
        return column.dictionary.size();
      }

      @Override
      public String get(int index)
      {
        return column.dictionary.get(index);
      }

      @Override
      public int indexOf(String value)
      {
        return column.dictionary.indexOf(value);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", column);
      }

      @Override
      public void close()
      {
        column.close();
      }
    });
  }

  @Override
  public ColumnValueSelector convertSortedSegmentRowValuesToMergedRowValues(
      int segmentIndex,
      ColumnValueSelector source
  )
  {
    return source;
  }

  @Override
  public void processMergedRow(ColumnValueSelector selector) throws IOException
  {
    serializer.serialize(selector);
  }

  @Override
  public void writeIndexes(@Nullable List<IntBuffer> segmentRowNumConversions)
  {
    // fields write their own indexes
  }

  @Override
  public boolean hasOnlyNulls()
  {
    return false;
  }

  @Override
  public ColumnDescriptor makeColumnDescriptor()
  {
    return new ColumnDescriptor.Builder()
        .setValueType(ValueType.COMPLEX)
        .setHasMultipleValues(false)
        .addSerde(ComplexColumnPartSerde.serializerBuilder()
                                        .withTypeName(NestedDataComplexTypeSerde.TYPE_NAME)
                                        .withDelegate(serializer)
                                        .build()
        )
        .build();
  }

  private boolean allNull(Indexed<String> dimValues)
  {
    for (int i = 0, size = dimValues.size(); i < size; i++) {
      if (dimValues.get(i) != null) {
        return false;
      }
    }
    return true;
  }
}
