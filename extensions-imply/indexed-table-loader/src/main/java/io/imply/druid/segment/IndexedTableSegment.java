/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.segment;

import io.imply.druid.segment.join.IndexedTableManager;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;

public class IndexedTableSegment implements Segment
{
  private final Segment delegate;
  private final IndexedTableManager manager;

  public IndexedTableSegment(IndexedTableManager manager, Segment actualSegment)
  {
    this.manager = manager;
    this.delegate = actualSegment;
  }

  @Override
  public SegmentId getId()
  {
    return delegate.getId();
  }

  @Override
  public Interval getDataInterval()
  {
    return delegate.getDataInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return delegate.asQueryableIndex();
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return delegate.asStorageAdapter();
  }

  @Nullable
  @Override
  public <T> T as(Class<T> aClass)
  {
    return delegate.as(aClass);
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
    // dropping the segment drops the table
    manager.dropIndexedTable(this);
  }
}
