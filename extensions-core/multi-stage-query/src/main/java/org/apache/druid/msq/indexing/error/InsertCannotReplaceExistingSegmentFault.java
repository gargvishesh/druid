/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.SegmentId;

import java.util.Objects;

public class InsertCannotReplaceExistingSegmentFault extends BaseMSQFault
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
