/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKeyDeserializerModule;
import io.imply.druid.talaria.frame.cluster.statistics.KeyCollectorFactory;
import io.imply.druid.talaria.frame.cluster.statistics.KeyCollectorSnapshotDeserializerModule;
import io.imply.druid.talaria.frame.cluster.statistics.KeyCollectors;
import io.imply.druid.talaria.indexing.error.CanceledFault;
import io.imply.druid.talaria.indexing.error.TalariaErrorReport;
import io.imply.druid.talaria.indexing.error.UnknownFault;
import io.imply.druid.talaria.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;

public class TalariaTasks
{
  /**
   * Message used by {@link #makeErrorReport} when no other message is known.
   */
  static final String GENERIC_QUERY_FAILED_MESSAGE = "Query failed";

  /**
   * Returns a decorated copy of an ObjectMapper, enabled for the classes
   * {@link io.imply.druid.talaria.frame.cluster.ClusterByKey} and
   * {@link io.imply.druid.talaria.frame.cluster.statistics.KeyCollector}.
   */
  static ObjectMapper decorateObjectMapperForClusterByKey(
      final ObjectMapper mapper,
      final RowSignature frameSignature,
      final ClusterBy clusterBy,
      final boolean aggregate
  )
  {
    final KeyCollectorFactory<?, ?> keyCollectorFactory =
        KeyCollectors.makeStandardFactory(clusterBy, frameSignature, aggregate);

    final ObjectMapper mapperCopy = mapper.copy();
    mapperCopy.registerModule(new ClusterByKeyDeserializerModule(frameSignature, clusterBy));
    mapperCopy.registerModule(new KeyCollectorSnapshotDeserializerModule(keyCollectorFactory));
    return mapperCopy;
  }

  /**
   * Returns the host:port from a {@link DruidNode}. Convenience method to make it easier to construct
   * {@link TalariaErrorReport} instances.
   */
  @Nullable
  static String getHostFromSelfNode(@Nullable final DruidNode selfNode)
  {
    return selfNode != null ? selfNode.getHostAndPortToUse() : null;
  }

  /**
   * Builds an error report from a possible controller error report and a possible worker error report. Both may be
   * null, in which case this function will return a report with {@link UnknownFault}.
   *
   * We only include a single {@link TalariaErrorReport} in the task report, because it's important that a query have
   * a single {@link io.imply.druid.talaria.indexing.error.TalariaFault} explaining why it failed. To aid debugging
   * in cases where we choose the controller error over the worker error, we'll log the worker error too, even though
   * it doesn't appear in the report.
   *
   * Logic: we prefer the controller exception unless it's {@link WorkerRpcFailedFault} or {@link CanceledFault}, in
   * which case we prefer the worker error report. This ensures we get the best, most useful exception even when the
   * controller cancels worker tasks after a failure. (As tasks are canceled one by one, worker -> worker and controller -> worker
   * RPCs to the canceled tasks will fail. We want to ignore these failed RPCs and get to the "true" error that
   * started it all.)
   */
  static TalariaErrorReport makeErrorReport(
      final String controllerTaskId,
      final String controllerHost,
      @Nullable TalariaErrorReport controllerErrorReport,
      @Nullable TalariaErrorReport workerErrorReport
  )
  {
    if (controllerErrorReport == null && workerErrorReport == null) {
      // Something went wrong, but we have no idea what.
      return TalariaErrorReport.fromFault(
          controllerTaskId,
          controllerHost,
          null,
          UnknownFault.forMessage(GENERIC_QUERY_FAILED_MESSAGE)
      );
    } else if (controllerErrorReport == null) {
      // workerErrorReport is nonnull.
      return workerErrorReport;
    } else {
      // controllerErrorReport is nonnull.

      // Pick the "best" error if both are set. See the javadoc for the logic we use. In these situations, we
      // expect the caller to also log the other one. (There is no logging in _this_ method, because it's a helper
      // function, and it's best if helper functions run quietly.)
      if (workerErrorReport != null && (controllerErrorReport.getFault() instanceof WorkerRpcFailedFault || controllerErrorReport.getFault() instanceof CanceledFault)) {
        return workerErrorReport;
      } else {
        return controllerErrorReport;
      }
    }
  }

  /**
   * Returns a string form of a {@link TalariaErrorReport} suitable for logging.
   */
  static String errorReportToLogMessage(final TalariaErrorReport errorReport)
  {
    final StringBuilder logMessage = new StringBuilder("Work failed");

    if (errorReport.getStageNumber() != null) {
      logMessage.append("; stage ").append(errorReport.getStageNumber());
    }

    logMessage.append("; task ").append(errorReport.getTaskId());

    if (errorReport.getHost() != null) {
      logMessage.append("; host ").append(errorReport.getHost());
    }

    logMessage.append(": ").append(errorReport.getFault().getCodeWithMessage());

    if (errorReport.getExceptionStackTrace() != null) {
      if (errorReport.getFault() instanceof UnknownFault) {
        // Log full stack trace for unknown faults.
        logMessage.append('\n').append(errorReport.getExceptionStackTrace());
      } else {
        // Log first line only (error class, message) for known faults, to avoid polluting logs.
        final String stackTrace = errorReport.getExceptionStackTrace();
        final int firstNewLine = stackTrace.indexOf('\n');

        logMessage.append(" (")
                  .append(firstNewLine > 0 ? stackTrace.substring(0, firstNewLine) : stackTrace)
                  .append(")");
      }
    }

    return logMessage.toString();
  }
}
