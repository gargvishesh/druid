/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.Segment;

import java.io.Closeable;
import java.util.Objects;

public class SegmentWithDescriptor implements Closeable
{
  private final ResourceHolder<? extends Segment> segmentHolder;
  private final SegmentDescriptor descriptor;

  public SegmentWithDescriptor(
      final ResourceHolder<? extends Segment> segmentHolder,
      final SegmentDescriptor descriptor
  )
  {
    this.segmentHolder = Preconditions.checkNotNull(segmentHolder, "segment");
    this.descriptor = Preconditions.checkNotNull(descriptor, "descriptor");
  }

  public Segment getOrLoadSegment()
  {
    return segmentHolder.get();
  }

  @Override
  public void close()
  {
    segmentHolder.close();
  }

  public SegmentDescriptor getDescriptor()
  {
    return descriptor;
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
    SegmentWithDescriptor that = (SegmentWithDescriptor) o;
    return Objects.equals(segmentHolder, that.segmentHolder) && Objects.equals(descriptor, that.descriptor);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentHolder, descriptor);
  }
}
