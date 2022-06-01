/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import io.imply.druid.talaria.framework.TalariaTestRunner;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

public class InsertLockPreemptedFaultTest extends TalariaTestRunner
{

  @Test
  public void testThrowsInsertLockPreemptedFault()
  {
    LockPreemptedHelper.preempt(true);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null "
                         + "group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedTalariaFault(InsertLockPreemptedFault.instance())
                     .verifyResults();

  }

  /**
   * Hack to allow the {@link io.imply.druid.talaria.framework.TalariaTestTaskActionClient} to determine whether
   * to grant or preempt the lock
   */
  public static class LockPreemptedHelper
  {
    private static boolean preempted = false;

    public static void preempt(final boolean preempted)
    {
      LockPreemptedHelper.preempted = preempted;
    }

    public static void throwIfPreempted()
    {
      if (preempted) {
        throw new ISE(
            "Segments[dummySegment] are not covered by locks[dummyLock] for task[dummyTask]"
        );
      }
    }
  }
}
