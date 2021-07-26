/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.loading;

import org.apache.druid.java.util.common.logger.Logger;

/**
 * A separate class to log segment lifecycle events in case we want to debug just the segment lifecycle
 */
public class SegmentLifecycleLogger extends Logger
{
  public static final Logger LOG = new SegmentLifecycleLogger();

  private SegmentLifecycleLogger()
  {
    super(SegmentLifecycleLogger.class);
  }
}
