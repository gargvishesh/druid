/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.samples.caffeine;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.utils.JvmUtils;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class CaffeineSampleStoreConfig
{
  @JsonProperty("expireTimeMinutes")
  private long expireTimeMinutes = TimeUnit.MINUTES.convert(1, TimeUnit.HOURS);

  @JsonProperty("maxSizeBytes")
  private long maxSizeBytes = JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes() / 5;

  public long getCacheExpireTimeMinutes()
  {
    return expireTimeMinutes;
  }

  public long getMaxSizeBytes()
  {
    return maxSizeBytes;
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
    CaffeineSampleStoreConfig that = (CaffeineSampleStoreConfig) o;
    return expireTimeMinutes == that.expireTimeMinutes && maxSizeBytes == that.maxSizeBytes;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expireTimeMinutes, maxSizeBytes);
  }
}
