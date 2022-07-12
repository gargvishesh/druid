/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.counters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.talaria.guice.TalariaIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class CountersSnapshotTreeTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper =
        TestHelper.makeJsonMapper().registerModules(new TalariaIndexingModule().getJacksonModules());

    final ChannelCounters channelCounters = new ChannelCounters();
    channelCounters.addFile(10, 13);
    channelCounters.setTotalFiles(14);

    final CounterSnapshotsTree snapshotsTree = new CounterSnapshotsTree();
    snapshotsTree.put(1, 2, new CounterSnapshots(ImmutableMap.of("ctr", channelCounters.snapshot())));

    final String json = mapper.writeValueAsString(snapshotsTree);
    final CounterSnapshotsTree snapshotsTree2 = mapper.readValue(json, CounterSnapshotsTree.class);

    Assert.assertEquals(snapshotsTree.copyMap(), snapshotsTree2.copyMap());
  }
}
