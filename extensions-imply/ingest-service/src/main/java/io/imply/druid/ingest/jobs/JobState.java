/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

public enum JobState
{
  STAGED,
  SCHEDULED,
  RUNNING,
  COMPLETE,
  CANCELLED,
  FAILED;

  @Nullable
  @JsonCreator
  public static JobState fromString(@Nullable String name)
  {
    if (name == null) {
      return null;
    }
    return valueOf(StringUtils.toUpperCase(name));
  }
}
