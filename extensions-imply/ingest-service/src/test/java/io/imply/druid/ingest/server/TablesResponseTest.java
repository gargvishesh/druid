/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.ingest.jobs.JobState;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.Action;
import org.junit.Assert;
import org.junit.Test;

public class TablesResponseTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    TablesResponse response = new TablesResponse(
        ImmutableList.of(
            new TableInfo("some_table", DateTimes.nowUtc(), ImmutableSet.of(Action.READ, Action.WRITE), null),
            new TableInfo(
                "other_table",
                DateTimes.nowUtc(),
                ImmutableSet.of(Action.READ),
                ImmutableList.of(
                    new TableInfo.JobStateCount(JobState.RUNNING, 10),
                    new TableInfo.JobStateCount(JobState.CANCELLED, 1000),
                    new TableInfo.JobStateCount(JobState.COMPLETE, 10000),
                    new TableInfo.JobStateCount(JobState.FAILED, 25)
                )
            )
        )
    );
    String serialized = MAPPER.writeValueAsString(response);
    TablesResponse andBack = MAPPER.readValue(serialized, TablesResponse.class);
    Assert.assertEquals(response, andBack);
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(TablesResponse.class).usingGetClass().verify();
  }
}
