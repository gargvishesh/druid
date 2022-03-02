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
import org.apache.druid.collections.SerializablePair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DistinctKeySnapshot implements KeyCollectorSnapshot
{
  private final List<SerializablePair<ClusterByKey, Long>> keys;
  private final int spaceReductionFactor;

  @JsonCreator
  DistinctKeySnapshot(
      @JsonProperty("keys") final List<SerializablePair<ClusterByKey, Long>> keys,
      @JsonProperty("spaceReductionFactor") final int spaceReductionFactor
  )
  {
    this.keys = Preconditions.checkNotNull(keys, "keys");
    this.spaceReductionFactor = spaceReductionFactor;
  }

  @JsonProperty
  public List<SerializablePair<ClusterByKey, Long>> getKeys()
  {
    return keys;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getSpaceReductionFactor()
  {
    return spaceReductionFactor;
  }

  public Map<ClusterByKey, Long> getKeysAsMap()
  {
    final Map<ClusterByKey, Long> keysMap = new HashMap<>();

    for (final SerializablePair<ClusterByKey, Long> key : keys) {
      keysMap.put(key.lhs, key.rhs);
    }

    return keysMap;
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
    DistinctKeySnapshot that = (DistinctKeySnapshot) o;

    // Not expected to be called in production, so it's OK that this calls getKeysAsMap() each time.
    return spaceReductionFactor == that.spaceReductionFactor && Objects.equals(getKeysAsMap(), that.getKeysAsMap());
  }

  @Override
  public int hashCode()
  {
    // Not expected to be called in production, so it's OK that this calls getKeysAsMap() each time.
    return Objects.hash(getKeysAsMap(), spaceReductionFactor);
  }
}
