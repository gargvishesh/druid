/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Preconditions;

import java.util.Map;

public class WarningCountersSnapshot
{
  private final Map<String, Long> warningCount;

  @JsonCreator
  public WarningCountersSnapshot(
      @JsonProperty("warningCount") Map<String, Long> warningCount
  )
  {
    this.warningCount = Preconditions.checkNotNull(warningCount);
  }

  @JsonProperty("warningCount")
  public Map<String, Long> getWarningCount()
  {
    return warningCount;
  }
}
