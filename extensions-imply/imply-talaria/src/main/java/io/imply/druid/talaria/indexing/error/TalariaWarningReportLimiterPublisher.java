/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.google.common.collect.ImmutableMap;
import io.imply.druid.talaria.exec.Limits;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TalariaWarningReportLimiterPublisher implements TalariaWarningReportPublisher
{

  final TalariaWarningReportPublisher delegate;
  final long totalLimit;
  final Map<String, Long> errorCodeToLimit;
  final ConcurrentHashMap<String, Long> errorCodeToCurrentCount = new ConcurrentHashMap<>();

  volatile long totalCount = 0L;

  final Object lock = new Object();

  public TalariaWarningReportLimiterPublisher(TalariaWarningReportPublisher delegate)
  {
    this(
        delegate,
        Limits.MAX_VERBOSE_WARNINGS,
        ImmutableMap.of(
            TalariaWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, Limits.MAX_VERBOSE_PARSE_EXCEPTIONS
        )
    );
  }

  public TalariaWarningReportLimiterPublisher(
      TalariaWarningReportPublisher delegate,
      long totalLimit,
      Map<String, Long> errorCodeToLimit
  )
  {
    this.delegate = delegate;
    this.errorCodeToLimit = errorCodeToLimit;
    this.totalLimit = totalLimit;
  }

  @Override
  public void publishException(int stageNumber, Throwable e)
  {
    String errorCode = TalariaErrorReport.getFaultFromException(e).getErrorCode();
    synchronized (lock) {
      totalCount = totalCount + 1;
      errorCodeToCurrentCount.compute(errorCode, (ignored, count) -> count == null ? 1L : count + 1);
    }

    if (totalLimit != -1 && totalCount > totalLimit) {
      return;
    }

    long limitForFault = errorCodeToLimit.getOrDefault(errorCode, -1L);
    synchronized (lock) {
      if (limitForFault != -1 && errorCodeToCurrentCount.getOrDefault(errorCode, 0L) > limitForFault) {
        return;
      }
    }
    delegate.publishException(stageNumber, e);
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }
}
