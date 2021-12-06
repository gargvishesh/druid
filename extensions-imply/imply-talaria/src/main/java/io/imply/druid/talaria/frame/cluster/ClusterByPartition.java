/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * TODO(gianm): Javadocs
 */
public class ClusterByPartition
{
  @Nullable
  private final ClusterByKey start;
  @Nullable
  private final ClusterByKey end;

  @JsonCreator
  public ClusterByPartition(
      @JsonProperty("start") @Nullable ClusterByKey start,
      @JsonProperty("end") @Nullable ClusterByKey end
  )
  {
    this.start = start;
    this.end = end;
  }

  /**
   * Get the starting key for this range. It is inclusive (the range *does* contain this key).
   *
   * Null means the range is unbounded at the start.
   */
  @JsonProperty
  @Nullable
  public ClusterByKey getStart()
  {
    return start;
  }

  /**
   * Get the ending key for this range. It is exclusive (the range *does not* contain this key).
   *
   * Null means the range is unbounded at the end.
   */
  @JsonProperty
  @Nullable
  public ClusterByKey getEnd()
  {
    return end;
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
    ClusterByPartition that = (ClusterByPartition) o;
    return Objects.equals(start, that.start) && Objects.equals(end, that.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(start, end);
  }

  @Override
  public String toString()
  {
    return "{" + start + " -> " + end + "}";
  }
}
