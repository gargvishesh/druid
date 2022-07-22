/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.talaria.guice.TalariaIndexingModule;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TalariaFaultSerdeTest
{
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    objectMapper = TestHelper.makeJsonMapper();
    objectMapper.registerModules(new TalariaIndexingModule().getJacksonModules());
  }

  @Test
  public void testFaultSerde() throws IOException
  {
    assertFaultSerde(new BroadcastTablesTooLargeFault(10));
    assertFaultSerde(CanceledFault.INSTANCE);
    assertFaultSerde(new CannotParseExternalDataFault("the message"));
    assertFaultSerde(new ColumnTypeNotSupportedFault("the column", null));
    assertFaultSerde(new ColumnTypeNotSupportedFault("the column", ColumnType.STRING_ARRAY));
    assertFaultSerde(new ColumnNameRestrictedFault("the column"));
    assertFaultSerde(new InsertCannotAllocateSegmentFault("the datasource", Intervals.ETERNITY));
    assertFaultSerde(new InsertCannotBeEmptyFault("the datasource"));
    assertFaultSerde(new InsertCannotOrderByDescendingFault("the column"));
    assertFaultSerde(
        new InsertCannotReplaceExistingSegmentFault(SegmentId.of("the datasource", Intervals.ETERNITY, "v1", 1))
    );
    assertFaultSerde(InsertLockPreemptedFault.INSTANCE);
    assertFaultSerde(InsertTimeNullFault.INSTANCE);
    assertFaultSerde(new InsertTimeOutOfBoundsFault(Intervals.ETERNITY));
    assertFaultSerde(new InvalidNullByteFault("the column"));
    assertFaultSerde(new NotEnoughMemoryFault(1000, 1, 2));
    assertFaultSerde(QueryNotSupportedFault.INSTANCE);
    assertFaultSerde(new RowTooLargeFault(1000));
    assertFaultSerde(new TaskStartTimeoutFault(10));
    assertFaultSerde(new TooManyBucketsFault(10));
    assertFaultSerde(new TooManyColumnsFault(10, 8));
    assertFaultSerde(new TooManyInputFilesFault(15, 10, 5));
    assertFaultSerde(new TooManyPartitionsFault(10));
    assertFaultSerde(new TooManyWarningsFault(10, "the error"));
    assertFaultSerde(new TooManyWorkersFault(10, 5));
    assertFaultSerde(UnknownFault.forMessage(null));
    assertFaultSerde(UnknownFault.forMessage("the message"));
    assertFaultSerde(new WorkerFailedFault("the worker task", "the error msg"));
    assertFaultSerde(new WorkerRpcFailedFault("the worker task"));
  }

  @Test
  public void testFaultEqualsAndHashCode()
  {
    for (Class<? extends TalariaFault> faultClass : TalariaIndexingModule.FAULT_CLASSES) {
      EqualsVerifier.forClass(faultClass).usingGetClass().verify();
    }
  }

  private void assertFaultSerde(final TalariaFault fault) throws IOException
  {
    final String json = objectMapper.writeValueAsString(fault);
    final TalariaFault fault2 = objectMapper.readValue(json, TalariaFault.class);
    Assert.assertEquals(json, fault, fault2);
  }
}
