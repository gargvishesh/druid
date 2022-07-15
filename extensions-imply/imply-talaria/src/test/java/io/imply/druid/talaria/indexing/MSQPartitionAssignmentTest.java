/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.collect.ImmutableMap;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.easymock.EasyMock.mock;

public class MSQPartitionAssignmentTest
{

  @Test(expected = NullPointerException.class)
  public void testNullPartition()
  {
    new MSQPartitionAssignment(null, Collections.emptyMap());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPartition()
  {
    Map<Integer, SegmentIdWithShardSpec> allocations = ImmutableMap.of(-1, mock(SegmentIdWithShardSpec.class));
    new MSQPartitionAssignment(ClusterByPartitions.oneUniversalPartition(), allocations);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(MSQPartitionAssignment.class)
                  .withNonnullFields("partitions", "allocations")
                  .usingGetClass()
                  .verify();
  }
}
