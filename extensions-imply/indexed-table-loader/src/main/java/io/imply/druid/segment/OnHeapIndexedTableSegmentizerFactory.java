/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.imply.druid.segment.join.IndexedTableManager;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.table.RowBasedIndexedTable;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentizerFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class OnHeapIndexedTableSegmentizerFactory implements SegmentizerFactory
{
  private static final byte CACHE_PREFIX = -0x01;

  private final IndexIO indexIO;
  private final IndexedTableManager manager;
  private final Set<String> keyColumns;

  @JsonCreator
  public OnHeapIndexedTableSegmentizerFactory(
      @JacksonInject IndexIO indexIO,
      @JacksonInject IndexedTableManager manager,
      @JsonProperty("keyColumns") Set<String> keyColumns
  )
  {
    this.indexIO = Preconditions.checkNotNull(indexIO, "IndexIO cannot be null");
    this.manager = Preconditions.checkNotNull(manager, "IndexedTableManager cannot be null");
    Preconditions.checkArgument(
        keyColumns != null && keyColumns.size() > 0,
        "'keyColumns' must be set"
    );
    this.keyColumns = keyColumns;
  }

  @JsonProperty("keyColumns")
  public Set<String> getKeyColumns()
  {
    return keyColumns;
  }

  @Override
  public Segment factorize(
      DataSegment dataSegment,
      File parentDir,
      boolean lazy,
      SegmentLazyLoadFailCallback loadFailed
  ) throws SegmentLoadingException
  {
    try {
      QueryableIndexSegment theSegment = new QueryableIndexSegment(
          indexIO.loadIndex(parentDir, lazy, loadFailed),
          dataSegment.getId()
      );

      IndexedTableSegment parappaTheWrapper = new IndexedTableSegment(manager, theSegment);
      manager.addIndexedTable(dataSegment.getDataSource(), parappaTheWrapper, (existingTable) -> {
        if (existingTable != null) {
          throw new IllegalStateException("OnHeapIndexedTable with multiple segments is not supported");
        }
        ArrayBasedTable table = new ArrayBasedTable(dataSegment.getDataSource(), theSegment);

        RowSignature.Builder sigBuilder = RowSignature.builder();
        QueryableIndexStorageAdapter adapter = (QueryableIndexStorageAdapter) theSegment.asStorageAdapter();
        sigBuilder.add(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG);
        for (String column : theSegment.asQueryableIndex().getColumnNames()) {
          final ColumnCapabilities capabilities = adapter.getColumnCapabilities(column);
          if (capabilities != null) {
            sigBuilder.add(column, capabilities.toColumnType());
          } else {
            sigBuilder.add(column, null);
          }
        }

        return new RowBasedIndexedTable<>(
            table.getEncodedRows(),
            table.getRowAdapter(),
            sigBuilder.build(),
            keyColumns,
            theSegment.getId().getVersion(),
            computeCacheKey(theSegment.getId())
        );
      });
      if (manager.containsIndexedTable(dataSegment.getDataSource())) {
        return parappaTheWrapper;
      } else {
        // if we failed to load the indexed table, or dropped it for data consistency concerns, use the regular segment
        return theSegment;
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "%s", e.getMessage());
    }
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
    OnHeapIndexedTableSegmentizerFactory that = (OnHeapIndexedTableSegmentizerFactory) o;
    return Objects.equals(keyColumns, that.keyColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(keyColumns);
  }

  @VisibleForTesting
  public static byte[] computeCacheKey(SegmentId segmentId)
  {
    CacheKeyBuilder keyBuilder = new CacheKeyBuilder(CACHE_PREFIX);
    return keyBuilder
        .appendLong(segmentId.getInterval().getStartMillis())
        .appendLong(segmentId.getInterval().getEndMillis())
        .appendString(segmentId.getVersion())
        .appendString(segmentId.getDataSource())
        .appendInt(segmentId.getPartitionNum())
        .build();
  }
}
