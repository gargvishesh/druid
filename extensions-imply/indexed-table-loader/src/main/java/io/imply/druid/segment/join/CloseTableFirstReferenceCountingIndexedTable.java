/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.segment.join;

import org.apache.druid.segment.ReferenceCountingCloseableObject;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.ReferenceCountingIndexedTable;
import org.apache.druid.timeline.SegmentId;

import java.util.concurrent.atomic.AtomicBoolean;

public class CloseTableFirstReferenceCountingIndexedTable extends ReferenceCountingIndexedTable
{
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final SegmentId segmentId;

  public CloseTableFirstReferenceCountingIndexedTable(IndexedTable indexedTable, SegmentId segmentId)
  {
    super(indexedTable);
    this.segmentId = segmentId;
  }

  public SegmentId getSegmentId()
  {
    return segmentId;
  }

  boolean isOlderThan(Segment s)
  {
    return s.getId().getVersion().compareTo(version()) > 0;
  }

  boolean isSameVersion(Segment s)
  {
    return s.getId().getVersion().compareTo(version()) == 0;
  }

  /**
   * Override {@link ReferenceCountingCloseableObject#close} so that close becomes a blocking operation.
   * This entire class should be removed once we move away from drop then load, and instead just use
   * {@link ReferenceCountingIndexedTable}
   */
  @Override
  public void close()
  {
    if (this.closed.compareAndSet(false, true)) {
      this.referents.awaitAdvance(this.referents.arriveAndDeregister());
    }
  }
}
