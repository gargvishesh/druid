/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import org.apache.druid.java.util.common.StringUtils;

/**
 * Exception that is conventionally thrown by workers when they call
 * {@link io.imply.druid.talaria.frame.write.FrameWriter#addSelection} and it returns false on an empty frame, or in
 * a situation where allocating a new frame is impractical.
 */
public class FrameRowTooLargeException extends RuntimeException
{
  private final long maxFrameSize;

  public FrameRowTooLargeException(final long maxFrameSize)
  {
    super(StringUtils.format("Row too large to add to frame (max frame size = %,d)", maxFrameSize));
    this.maxFrameSize = maxFrameSize;
  }

  public long getMaxFrameSize()
  {
    return maxFrameSize;
  }
}
