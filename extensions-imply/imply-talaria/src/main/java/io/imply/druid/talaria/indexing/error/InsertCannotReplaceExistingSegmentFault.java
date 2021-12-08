/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.SegmentId;

public class InsertCannotReplaceExistingSegmentFault extends BaseTalariaFault
{
  static final String CODE = "InsertCannotReplaceExistingSegment";

  private final SegmentId segmentId;

  public InsertCannotReplaceExistingSegmentFault(@JsonProperty("segmentId") SegmentId segmentId)
  {
    super(
        CODE,
        "Cannot replace existing segment [%s] because it is not within the "
        + "bounds specified by replaceExistingTimeChunks",
        segmentId
    );
    this.segmentId = segmentId;
  }

  @JsonProperty
  public SegmentId getSegmentId()
  {
    return segmentId;
  }
}
