/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.timeline.SegmentId;

public class SegmentNotEvictableException extends IAE
{
  public SegmentNotEvictableException(SegmentId segmentId)
  {
    super("[%s] is not evictable", segmentId);
  }
}
