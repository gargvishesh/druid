/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment;

import com.google.common.base.Preconditions;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;

public class VirtualSegment implements Segment
{
  private final DataSegment dataSegment;
  private final boolean lazy;
  private final SegmentLazyLoadFailCallback loadFailedCallback;
  private Segment realSegment;

  public VirtualSegment(DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback loadFailedCallback)
  {
    this.dataSegment = segment;
    this.lazy = lazy;
    this.loadFailedCallback = loadFailedCallback;
  }

  @Override
  public SegmentId getId()
  {
    return dataSegment.getId();
  }

  @Override
  public Interval getDataInterval()
  {
    return dataSegment.getInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    checkQueryable();
    return realSegment.asQueryableIndex();
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    checkQueryable();
    return realSegment.asStorageAdapter();
  }

  @Override
  public void close() throws IOException
  {
    if (null != realSegment) {
      realSegment.close();
    }
    realSegment = null;
  }

  public DataSegment asDataSegment()
  {
    return dataSegment;
  }

  public boolean isLazy()
  {
    return lazy;
  }

  public SegmentLazyLoadFailCallback getLoadFailedCallback()
  {
    return loadFailedCallback;
  }

  public Segment getRealSegment()
  {
    return realSegment;
  }

  public void setRealSegment(Segment realSegment)
  {
    this.realSegment = realSegment;
  }

  private void checkQueryable()
  {
    Preconditions.checkNotNull(
        realSegment,
        "Segment [%s] being queried has not been downloaded or has been evicted",
        this.getId()
    );
  }
}
