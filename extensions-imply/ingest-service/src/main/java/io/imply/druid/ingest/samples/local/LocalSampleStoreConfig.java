/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.samples.local;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class LocalSampleStoreConfig
{
  private String baseDir;

  @JsonCreator
  public LocalSampleStoreConfig(
      @JsonProperty("baseDir") String baseDir
  )
  {
    this.baseDir = baseDir;
  }

  @JsonProperty
  public String getBaseDir()
  {
    return baseDir;
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
    LocalSampleStoreConfig that = (LocalSampleStoreConfig) o;
    return Objects.equals(baseDir, that.baseDir);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseDir);
  }
}
