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

import java.util.List;

public class DistinctKeySnapshot implements KeyCollectorSnapshot
{
  private final List<ClusterByKey> keys;
  private final int spaceReductionFactor;

  @JsonCreator
  DistinctKeySnapshot(
      @JsonProperty("keys") final List<ClusterByKey> keys,
      @JsonProperty("spaceReductionFactor") final int spaceReductionFactor
  )
  {
    this.keys = Preconditions.checkNotNull(keys, "keys");
    this.spaceReductionFactor = spaceReductionFactor;
  }

  @JsonProperty
  public List<ClusterByKey> getKeys()
  {
    return keys;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getSpaceReductionFactor()
  {
    return spaceReductionFactor;
  }
}
