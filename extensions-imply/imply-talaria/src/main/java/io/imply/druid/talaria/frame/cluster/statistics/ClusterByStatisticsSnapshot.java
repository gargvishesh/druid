/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ClusterByStatisticsSnapshot
{
  private final List<Bucket> buckets;

  // TODO(gianm): hack alert: see note for hasMultipleValues in ClusterByStatisticsCollectorImpl
  private final Set<Integer> hasMultipleValues;

  @JsonCreator
  ClusterByStatisticsSnapshot(
      @JsonProperty("buckets") final List<Bucket> buckets,
      @JsonProperty("hasMultipleValues") @Nullable final Set<Integer> hasMultipleValues
  )
  {
    this.buckets = Preconditions.checkNotNull(buckets, "buckets");
    this.hasMultipleValues = hasMultipleValues != null ? hasMultipleValues : Collections.emptySet();
  }

  public static ClusterByStatisticsSnapshot empty()
  {
    return new ClusterByStatisticsSnapshot(Collections.emptyList(), null);
  }

  @JsonProperty("buckets")
  List<Bucket> getBuckets()
  {
    return buckets;
  }

  @JsonProperty("hasMultipleValues")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Set<Integer> getHasMultipleValues()
  {
    return hasMultipleValues;
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
    ClusterByStatisticsSnapshot that = (ClusterByStatisticsSnapshot) o;
    return Objects.equals(buckets, that.buckets) && Objects.equals(
        hasMultipleValues,
        that.hasMultipleValues
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(buckets, hasMultipleValues);
  }

  static class Bucket
  {
    private final ClusterByKey bucketKey;
    private final KeyCollectorSnapshot keyCollectorSnapshot;

    @JsonCreator
    public Bucket(
        @JsonProperty("bucketKey") ClusterByKey bucketKey,
        @JsonProperty("data") KeyCollectorSnapshot keyCollectorSnapshot
    )
    {
      this.bucketKey = Preconditions.checkNotNull(bucketKey, "bucketKey");
      this.keyCollectorSnapshot = Preconditions.checkNotNull(keyCollectorSnapshot, "data");
    }

    @JsonProperty
    public ClusterByKey getBucketKey()
    {
      return bucketKey;
    }

    @JsonProperty("data")
    public KeyCollectorSnapshot getKeyCollectorSnapshot()
    {
      return keyCollectorSnapshot;
    }
  }
}
