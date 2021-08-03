/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.utils.JvmUtils;

/**
 * Any configuration specific to the virtual segment extension goes in here.
 */
public class VirtualSegmentConfig
{
  private static final int DEFAULT_DOWNLOAD_THREADS = 5;
  private static final long DEFAULT_DOWNLOAD_DELAY_MS = 10;

  @JsonProperty
  private final int downloadThreads;

  @JsonProperty
  private final long downloadDelayMs;

  @JsonCreator
  public VirtualSegmentConfig(
      @JsonProperty("downloadThreads") Integer downloadThreads,
      @JsonProperty("downloadDelayMs") Long downloadDelayMs
  )
  {
    this.downloadThreads = downloadThreads != null ? downloadThreads : JvmUtils.getRuntimeInfo().getAvailableProcessors();
    this.downloadDelayMs = downloadDelayMs != null ? downloadDelayMs : DEFAULT_DOWNLOAD_DELAY_MS;
  }

  @JsonProperty
  public int getDownloadThreads()
  {
    return downloadThreads;
  }

  @JsonProperty
  public long getDownloadDelayMs()
  {
    return downloadDelayMs;
  }
}
