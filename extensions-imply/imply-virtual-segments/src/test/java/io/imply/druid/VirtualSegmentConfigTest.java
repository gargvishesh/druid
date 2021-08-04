/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
