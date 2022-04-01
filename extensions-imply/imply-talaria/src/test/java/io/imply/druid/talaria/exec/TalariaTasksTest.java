/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import io.imply.druid.talaria.indexing.error.TalariaErrorReport;
import io.imply.druid.talaria.indexing.error.TooManyColumnsFault;
import io.imply.druid.talaria.indexing.error.TooManyWorkersFault;
import io.imply.druid.talaria.indexing.error.UnknownFault;
import io.imply.druid.talaria.indexing.error.WorkerRpcFailedFault;
import org.junit.Assert;
import org.junit.Test;

public class TalariaTasksTest
{
  private static final String CONTROLLER_ID = "controller-id";
  private static final String WORKER_ID = "worker-id";
  private static final String CONTROLLER_HOST = "controller-host";
  private static final String WORKER_HOST = "worker-host";

  @Test
  public void test_makeErrorReport_allNull()
  {
    Assert.assertEquals(
        TalariaErrorReport.fromFault(
            CONTROLLER_ID,
            CONTROLLER_HOST,
            null,
            UnknownFault.forMessage(TalariaTasks.GENERIC_QUERY_FAILED_MESSAGE)
        ),
        TalariaTasks.makeErrorReport(CONTROLLER_ID, CONTROLLER_HOST, null, null)
    );
  }

  @Test
  public void test_makeErrorReport_controllerOnly()
  {
    final TalariaErrorReport controllerReport = TalariaTasks.makeErrorReport(
        CONTROLLER_ID,
        CONTROLLER_HOST,
        TalariaErrorReport.fromFault(CONTROLLER_ID, CONTROLLER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(controllerReport, TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, null));
  }

  @Test
  public void test_makeErrorReport_workerOnly()
  {
    final TalariaErrorReport workerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(workerReport, TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, null, workerReport));
  }

  @Test
  public void test_makeErrorReport_controllerPreferred()
  {
    final TalariaErrorReport controllerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyWorkersFault(2, 20)),
        null
    );

    final TalariaErrorReport workerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(
        controllerReport,
        TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, workerReport)
    );
  }

  @Test
  public void test_makeErrorReport_workerPreferred()
  {
    final TalariaErrorReport controllerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new WorkerRpcFailedFault(WORKER_ID)),
        null
    );

    final TalariaErrorReport workerReport = TalariaTasks.makeErrorReport(
        WORKER_ID,
        WORKER_HOST,
        TalariaErrorReport.fromFault(WORKER_ID, WORKER_HOST, null, new TooManyColumnsFault(1, 10)),
        null
    );

    Assert.assertEquals(
        workerReport,
        TalariaTasks.makeErrorReport(WORKER_ID, WORKER_HOST, controllerReport, workerReport)
    );
  }
}
