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

import java.util.Objects;

public class InsertCannotReplaceExistingSegmentFault extends BaseTalariaFault
{
  static final String CODE = "InsertCannotReplaceExistingSegment";

  private final String segmentId;

  public InsertCannotReplaceExistingSegmentFault(@JsonProperty("segmentId") String segmentId)
  {
    super(
        CODE,
        "Cannot replace existing segment [%s] because it is not within the "
        + "bounds specified by replaceExistingTimeChunks",
        segmentId
    );
    this.segmentId = segmentId;
  }

  public InsertCannotReplaceExistingSegmentFault(final SegmentId segmentId)
  {
    this(segmentId.toString());
  }

  @JsonProperty
  public String getSegmentId()
  {
    return segmentId;
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
    if (!super.equals(o)) {
      return false;
    }
    InsertCannotReplaceExistingSegmentFault that = (InsertCannotReplaceExistingSegmentFault) o;
    return Objects.equals(segmentId, that.segmentId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), segmentId);
  }
}
