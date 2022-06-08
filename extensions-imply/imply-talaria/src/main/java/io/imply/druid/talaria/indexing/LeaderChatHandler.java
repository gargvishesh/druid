/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import io.imply.druid.talaria.exec.Leader;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsSnapshot;
import io.imply.druid.talaria.indexing.error.TalariaErrorReport;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.server.security.Action;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

public class LeaderChatHandler implements ChatHandler
{
  private final Leader leader;
  private final TalariaControllerTask task;
  private final TaskToolbox toolbox;

  public LeaderChatHandler(TaskToolbox toolbox, Leader leader)
  {
    this.leader = leader;
    this.task = leader.task();
    this.toolbox = toolbox;
  }

  /**
   * Used by subtasks to post {@link ClusterByStatisticsSnapshot} for shuffling stages.
   *
   * See {@link io.imply.druid.talaria.exec.LeaderClient#postKeyStatistics} for the client-side code that calls this API.
   */
  @POST
  @Path("/keyStatistics/{queryId}/{stageNumber}/{workerNumber}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostKeyStatistics(
      final Object keyStatisticsObject,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("workerNumber") final int workerNumber,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    leader.updateStatus(stageNumber, workerNumber, keyStatisticsObject);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * Used by subtasks to post system errors. Note that the errors are organized by taskId, not by query/stage/worker,
   * because system errors are associated with a task rather than a specific query/stage/worker execution context.
   *
   * See {@link io.imply.druid.talaria.exec.LeaderClient#postWorkerError} for the client-side code that calls this API.
   */
  @POST
  @Path("/workerError/{taskId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostWorkerError(
      final TalariaErrorReport errorReport,
      @PathParam("taskId") final String taskId,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    leader.workerError(errorReport);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * Used by subtasks to post system warnings.
   *
   * See {@link io.imply.druid.talaria.exec.LeaderClient#postWorkerWarning} for the client-side code that calls this API.
   */
  @POST
  @Path("/workerWarning/{taskId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostWorkerWarning(
      final List<TalariaErrorReport> errorReport,
      @PathParam("taskId") final String taskId,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    leader.workerWarning(errorReport);
    return Response.status(Response.Status.ACCEPTED).build();
  }


  /**
   * Used by subtasks to post {@link TalariaCountersSnapshot} periodically.
   *
   * See {@link io.imply.druid.talaria.exec.LeaderClient#postCounters} for the client-side code that calls this API.
   */
  @POST
  @Path("/counters/{taskId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostCounters(
      final TalariaCountersSnapshot.WorkerCounters workerSnapshot,
      @PathParam("taskId") final String taskId,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    leader.updateCounters(taskId, workerSnapshot);
    return Response.status(Response.Status.OK).build();
  }

  /**
   * Used by subtasks to post notifications that their results are ready.
   *
   * See {@link io.imply.druid.talaria.exec.LeaderClient#postResultsComplete} for the client-side code that calls this API.
   */
  @POST
  @Path("/resultsComplete/{queryId}/{stageNumber}/{workerNumber}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostResultsComplete(
      final Object resultObject,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("workerNumber") final int workerNumber,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    leader.resultsComplete(queryId, stageNumber, workerNumber, resultObject);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link io.imply.druid.talaria.exec.LeaderClient#getTaskList} for the client-side code that calls this API.
   */
  @GET
  @Path("/taskList")
  @Produces(MediaType.APPLICATION_JSON)
  public Response httpGetTaskList(@Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());

    return Response.ok(new TalariaTaskList(leader.getTaskIds().orElse(null))).build();
  }

  /**
   * See {@link org.apache.druid.indexing.overlord.RemoteTaskRunner#streamTaskReports} for the client-side code that
   * calls this API.
   */
  @GET
  @Path("/liveReports")
  @Produces(MediaType.APPLICATION_JSON)
  public Response httpGetLiveReports(@Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    final Map<String, TaskReport> reports = leader.liveReports();
    if (reports == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    return Response.ok(reports).build();
  }
}
