/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.segment.join;

import com.google.inject.Inject;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;

import java.util.Optional;

/**
 * A {@link JoinableFactory} of {@link org.apache.druid.segment.join.table.RowBasedIndexedTable} for
 * {@link GlobalTableDataSource}
 */
public class IndexedTableJoinableFactory implements JoinableFactory
{
  private final IndexedTableManager indexedTableManager;

  @Inject
  IndexedTableJoinableFactory(IndexedTableManager indexedTableManager)
  {
    this.indexedTableManager = indexedTableManager;
  }

  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    GlobalTableDataSource indexedTableDataSource = (GlobalTableDataSource) dataSource;
    return indexedTableManager.containsIndexedTable(indexedTableDataSource.getName());
  }

  @Override
  public Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition)
  {
    GlobalTableDataSource indexedTableDataSource = (GlobalTableDataSource) dataSource;
    if (condition.canHashJoin()) {
      String tableName = indexedTableDataSource.getName();
      Optional<IndexedTable> table = Optional.ofNullable(indexedTableManager.getIndexedTable(tableName));
      return table.map(IndexedTableJoinable::new);
    }
    return Optional.empty();
  }

  @Override
  public Optional<byte[]> computeJoinCacheKey(
      DataSource dataSource,
      JoinConditionAnalysis condition
  )
  {
    GlobalTableDataSource indexedTableDataSource = (GlobalTableDataSource) dataSource;
    if (condition.canHashJoin()) {
      String tableName = indexedTableDataSource.getName();
      Optional<IndexedTable> table = Optional.ofNullable(indexedTableManager.getIndexedTable(tableName));
      return table.filter(IndexedTable::isCacheable).map(IndexedTable::computeCacheKey);
    }
    return Optional.empty();
  }
}
