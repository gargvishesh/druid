/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.exec.LeaderClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class TalariaWarningReportPublisher implements Closeable
{
  @GuardedBy("lock")
  private final List<TalariaErrorReport> unflushedReports = new ArrayList<>();
  private final Object lock = new Object();

  private final String leaderId;
  private final String workerId;
  private final String taskId;
  @Nullable
  private final String host;
  private final Integer stageNumber;
  private final LeaderClient leaderClient;
  @Nullable
  private ExecutorService periodicWarningsFlusherExec = null;

  private final long FREQUENCY_CHECK_JITTER_MS = 30L;

  public TalariaWarningReportPublisher(
      final String leaderId,
      final String workerId,
      final LeaderClient leaderClient,
      final String taskId,
      @Nullable final String host,
      final Integer stageNumber,
      final long flushIntervalMs
  )
  {
    this.leaderId = leaderId;
    this.workerId = workerId;
    this.leaderClient = leaderClient;
    this.taskId = taskId;
    this.host = host;
    this.stageNumber = stageNumber;

    if (flushIntervalMs != -1) {
      if (flushIntervalMs <= 0) {
        throw new ISE("flushIntervalMs can be a positive number to denote the period at which the warnings are "
                      + "published or -1 to prevent automatic flushing of the warnings");
      }

      this.periodicWarningsFlusherExec = Execs.singleThreaded("periodic-warnings-flusher-%s");
      periodicWarningsFlusherExec.submit(() -> {
        while (true) {

        }
      });
    }
  }

  @VisibleForTesting
  void periodicWarningsFlusherRunnable(final long flushIntervalMs)
  {
    while (true) {
      long sleepTimeMillis = flushIntervalMs;
      if (sleepTimeMillis > FREQUENCY_CHECK_JITTER_MS) {
        sleepTimeMillis += ThreadLocalRandom.current()
                                            .nextLong(-FREQUENCY_CHECK_JITTER_MS, 2 * FREQUENCY_CHECK_JITTER_MS);
      }

      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException e) {
        break;
      }
    }
  }

  public void publishException(Throwable e)
  {
    // TODO: Chomp the exception stack trace if it is more than a predetermined size
    synchronized (lock) {
      unflushedReports.add(TalariaErrorReport.fromException(taskId, host, stageNumber, e));
    }

  }

  public void flush()
  {
    synchronized (lock) {
      leaderClient.postWorkerWarning(leaderId, workerId, unflushedReports);
      unflushedReports.clear();
    }
  }

  @Override
  public void close()
  {
    flush();
  }
}
