/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid;

import org.apache.druid.utils.JvmUtils;
import org.junit.Assert;
import org.junit.Test;

public class VirtualSegmentConfigTest
{
  @Test
  public void testDefaultThreads()
  {
    VirtualSegmentConfig config = new VirtualSegmentConfig(null, 5L);
    Assert.assertEquals(JvmUtils.getRuntimeInfo().getAvailableProcessors(), config.getDownloadThreads());
  }

  @Test
  public void testNoDefaultThreads()
  {
    VirtualSegmentConfig config = new VirtualSegmentConfig(10, 5L);
    Assert.assertEquals(10, config.getDownloadThreads());
  }

  @Test
  public void testDefaultDownloadDelay()
  {
    VirtualSegmentConfig config = new VirtualSegmentConfig(10, null);
    Assert.assertEquals(10, config.getDownloadDelayMs());
  }

  @Test
  public void testNoDefaultDownloadDelay()
  {
    VirtualSegmentConfig config = new VirtualSegmentConfig(10, 5L);
    Assert.assertEquals(5L, config.getDownloadDelayMs());
  }
}
