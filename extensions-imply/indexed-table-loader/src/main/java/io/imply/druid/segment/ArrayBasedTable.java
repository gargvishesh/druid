/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.segment;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.chrono.ISOChronology;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

/**
 * A table that is backed by an array for fast access.
 * Low cardinality columns are backed by bitmaps for faster access. At some point this should be
 * consolidated with core Druid.
 *
 * The APIs to this class are designed so that it can be loaded as an IndexedTable
 */
public class ArrayBasedTable
{
  private static final Logger LOG = new Logger(ArrayBasedTable.class);

  /**
   * A 2D array storing the dimension to the list of unique values for a dimension.
   * name -> Pair(position, BiMap)
   *
   * For example a table called foo
   * col1 col2 col3
   *  A    cat  12
   *  B    cat  23
   *  C    dog  23
   *
   *  will produce a {@link #dimensionDictionary} that looks like
   *  col1 -> (0, {("A", 0), ("B", 1), ("C", 2)})
   *  col2 -> (1, {("cat", 0), ("dog", 1)})
   *  col3 -> (2, {(12, 0), (23, 1)})
   *
   *  NOTE the types are are based on the provided schema in the dimension spec.
   *  The position is needed so that we can look up the value in the row.
   */
  private final Map<String, Pair<Integer, Int2ObjectMap<Object>>> dimensionDictionary;

  /**
   * A list of rows. Each row is a Pair of timestamp and a list of encoded values.
   *
   * For the exmaple table in {@link #dimensionDictionary}
   * col1 col2 col3
   * A    cat  12
   * B    cat  23
   * C    dog  23
   *
   * {@code #encodedRows} will look like
   * [[0, 0, 0], [1, 0, 1], [2, 1, 1]]
   */
  private final ArrayList<IntArrayList> encodedRows;

  public ArrayBasedTable(String dataSource, QueryableIndexSegment segment)
  {
    final QueryableIndex index = segment.asQueryableIndex();
    final QueryableIndexStorageAdapter adapter = (QueryableIndexStorageAdapter) segment.asStorageAdapter();

    final List<String> columnNames = ImmutableList.<String>builder().add(ColumnHolder.TIME_COLUMN_NAME)
                                                                    .addAll(index.getColumnNames()).build();
    Map<String, Pair<Integer, BiMap<Object, Integer>>> localDimensionDictionary =
        Maps.newHashMapWithExpectedSize(columnNames.size());

    encodedRows = new ArrayList<>();

    LOG.info("Creating Array based table");

    // like the dump segment tool, but loaded into this array table insead
    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(null),
        index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final Sequence<Integer> sequence = Sequences.map(
        cursors,
        cursor -> {
          int rowNumber = 0;
          ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
          final List<BaseObjectColumnValueSelector> selectors = columnNames
              .stream()
              .map(columnSelectorFactory::makeColumnValueSelector)
              .collect(Collectors.toList());

          while (!cursor.isDone()) {
            IntArrayList encodedRow = new IntArrayList(columnNames.size());

            for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
              final String columnName = columnNames.get(columnIndex);
              Pair<Integer, BiMap<Object, Integer>> dimIndexAndDict =
                  localDimensionDictionary.computeIfAbsent(
                      columnName,
                      d -> Pair.of(localDimensionDictionary.size(), HashBiMap.create())
                  );
              final Object value = selectors.get(columnIndex).getObject();
              int dimIndex = dimIndexAndDict.getLeft();

              ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(columnName);
              Map<Object, Integer> dimDict = dimIndexAndDict.getRight();
              if (capabilities.hasMultipleValues().isTrue()) {
                // coerce multi-value columns to nulls for now
                int dictVal = dimDict.computeIfAbsent(null, d -> dimDict.size());
                encodedRow.add(dimIndex, dictVal);
              } else {
                int dictVal = dimDict.computeIfAbsent(value, d -> dimDict.size());
                encodedRow.add(dimIndex, dictVal);
              }
            }
            encodedRows.add(encodedRow);
            if (rowNumber % 100_000 == 0) {
              if (rowNumber == 0) {
                LOG.info("Read first row for indexed table %s", dataSource);
              } else {
                LOG.info("Read row %s for indexed table %s", rowNumber, dataSource);
              }
            }
            rowNumber++;
            cursor.advance();
          }
          return rowNumber;
        }
    );

    Integer totalRows = sequence.accumulate(0, (accumulated, in) -> accumulated += in);

    dimensionDictionary = Maps.newHashMapWithExpectedSize(localDimensionDictionary.size());
    localDimensionDictionary.forEach((key, value) -> dimensionDictionary.put(
        key,
        Pair.of(
            value.getLeft(),
            new Int2ObjectLinkedOpenHashMap<>(value.getRight().inverse())
        )
    ));
    LOG.info("Created ArrayBasedTable with %s rows.", totalRows);
  }

  public ArrayList<IntArrayList> getEncodedRows()
  {
    return encodedRows;
  }

  public RowAdapter<IntArrayList> getRowAdapter()
  {
    return new RowAdapter<IntArrayList>()
    {
      @Override
      public ToLongFunction<IntArrayList> timestampFunction()
      {
        Pair<Integer, Int2ObjectMap<Object>> dimIndexAndDict = dimensionDictionary.get(ColumnHolder.TIME_COLUMN_NAME);
        int columnIndex = dimIndexAndDict.getLeft();
        Int2ObjectMap<Object> encodedMap = dimIndexAndDict.getRight();
        return row -> {
          int encodedVal = row.getInt(columnIndex);
          return (long) encodedMap.get(encodedVal);
        };
      }

      @Override
      public Function<IntArrayList, Object> columnFunction(@NotNull String columnName)
      {
        Pair<Integer, Int2ObjectMap<Object>> dimIndexAndDict = dimensionDictionary.get(columnName);
        /* If the column doesn't exist. */
        if (dimIndexAndDict == null) {
          return row -> null;
        }
        int columnIndex = dimIndexAndDict.getLeft();
        Int2ObjectMap<Object> encodedMap = dimIndexAndDict.getRight();
        return row -> {
          int encodedVal = row.getInt(columnIndex);
          return encodedMap.get(encodedVal);
        };
      }
    };
  }
}
