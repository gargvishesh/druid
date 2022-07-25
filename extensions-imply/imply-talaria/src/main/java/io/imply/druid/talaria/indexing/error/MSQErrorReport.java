/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.imply.druid.talaria.frame.cluster.statistics.TooManyBucketsException;
import io.imply.druid.talaria.frame.processor.FrameRowTooLargeException;
import io.imply.druid.talaria.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.util.Objects;

public class MSQErrorReport
{
  private final String taskId;
  @Nullable
  private final String host;
  @Nullable
  private final Integer stageNumber;
  private final TalariaFault error;
  @Nullable
  private final String exceptionStackTrace;

  @JsonCreator
  MSQErrorReport(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("host") @Nullable final String host,
      @JsonProperty("stageNumber") final Integer stageNumber,
      @JsonProperty("error") final TalariaFault fault,
      @JsonProperty("exceptionStackTrace") @Nullable final String exceptionStackTrace
  )
  {
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
    this.host = host;
    this.stageNumber = stageNumber;
    this.error = Preconditions.checkNotNull(fault, "error");
    this.exceptionStackTrace = exceptionStackTrace;
  }

  public static MSQErrorReport fromFault(
      final String taskId,
      @Nullable final String host,
      @Nullable final Integer stageNumber,
      final TalariaFault fault
  )
  {
    return new MSQErrorReport(taskId, host, stageNumber, fault, null);
  }

  public static MSQErrorReport fromException(
      final String taskId,
      @Nullable final String host,
      @Nullable final Integer stageNumber,
      final Throwable e
  )
  {
    return new MSQErrorReport(
        taskId,
        host,
        stageNumber,
        getFaultFromException(e),
        Throwables.getStackTraceAsString(e)
    );
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getHost()
  {
    return host;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty("error")
  public TalariaFault getFault()
  {
    return error;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExceptionStackTrace()
  {
    return exceptionStackTrace;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MSQErrorReport that = (MSQErrorReport) o;
    return Objects.equals(taskId, that.taskId)
           && Objects.equals(host, that.host)
           && Objects.equals(stageNumber, that.stageNumber)
           && Objects.equals(error, that.error)
           && Objects.equals(exceptionStackTrace, that.exceptionStackTrace);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, host, error, exceptionStackTrace);
  }

  @Override
  public String toString()
  {
    return "MSQErrorReport{" +
           "taskId='" + taskId + '\'' +
           ", host='" + host + '\'' +
           ", stageNumber=" + stageNumber +
           ", error=" + error +
           ", exceptionStackTrace='" + exceptionStackTrace + '\'' +
           '}';
  }

  /**
   * Magical code that extracts a useful fault from an exception, even if that exception is not necessarily a
   * {@link TalariaException}. This method walks through the causal chain, and also "knows" about various exception
   * types thrown by other Druid code.
   */
  public static TalariaFault getFaultFromException(@Nullable final Throwable e)
  {
    // Unwrap exception wrappers to find an underlying fault. The assumption here is that the topmost recognizable
    // exception should be used to generate the fault code for the entire report.

    Throwable cause = e;

    // This method will grow as we try to add more faults and exceptions
    // One way of handling this would be to extend the faults to have a method like
    // public TalariaFault fromException(@Nullable Throwable e) which returns the specific fault if it can be reconstructed
    // from the exception or null. Then instead of having a case per exception, we can have a case per fault, which
    // should be cool because there is a 1:1 mapping between faults and exceptions (apart from the more geeneric
    // UnknownFaults and TalariaExceptions)
    while (cause != null) {

      if (cause instanceof TalariaException) {
        return ((TalariaException) cause).getFault();
      } else if (cause instanceof ParseException) {
        return new CannotParseExternalDataFault(cause.getMessage());
      } else if (cause instanceof UnsupportedColumnTypeException) {
        final UnsupportedColumnTypeException unsupportedColumnTypeException = (UnsupportedColumnTypeException) cause;
        return new ColumnTypeNotSupportedFault(
            unsupportedColumnTypeException.getColumnName(),
            unsupportedColumnTypeException.getColumnType()
        );
      } else if (cause instanceof TooManyBucketsException) {
        return new TooManyBucketsFault(((TooManyBucketsException) cause).getMaxBuckets());
      } else if (cause instanceof FrameRowTooLargeException) {
        return new RowTooLargeFault(((FrameRowTooLargeException) cause).getMaxFrameSize());
      } else {
        cause = cause.getCause();
      }
    }

    return UnknownFault.forException(e);
  }
}
