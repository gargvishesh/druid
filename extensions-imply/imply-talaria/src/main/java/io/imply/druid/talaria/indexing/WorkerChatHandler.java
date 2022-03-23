/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import io.imply.druid.talaria.exec.Worker;
import io.imply.druid.talaria.exec.WorkerImpl;
import io.imply.druid.talaria.kernel.StageId;
import io.imply.druid.talaria.kernel.WorkOrder;
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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class WorkerChatHandler implements ChatHandler
{
  private final Worker worker;
  private final TalariaWorkerTask task;
  private final TaskToolbox toolbox;

  public WorkerChatHandler(TaskToolbox toolbox, WorkerImpl worker)
  {
    this.worker = worker;
    this.task = worker.task();
    this.toolbox = toolbox;
  }

  /**
   * See {@link io.imply.druid.talaria.exec.WorkerClient#getChannelData} for the client-side code that calls this API.
   */
  @GET
  @Path("/channels/{queryId}/{stageNumber}/{partitionNumber}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response httpGetChannelData(
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("partitionNumber") final int partitionNumber,
      @QueryParam("offset") final long offset,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    return worker.readChannel(queryId, stageNumber, partitionNumber, offset);
  }

  /**
   * See {@link io.imply.druid.talaria.exec.WorkerClient#postWorkOrder} for the client-side code that calls this API.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/workOrder")
  public Response httpPostWorkOrder(final WorkOrder workOrder, @Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    worker.postWorkOrder(workOrder);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link io.imply.druid.talaria.exec.WorkerClient#postResultPartitionBoundaries} for the client-side code that calls this API.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/resultPartitionBoundaries/{queryId}/{stageNumber}")
  public Response httpPostResultPartitionBoundaries(
      final Object stagePartitionBoundariesObject,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    if (worker.postResultPartitionBoundaries(stagePartitionBoundariesObject, queryId, stageNumber)) {
      return Response.status(Response.Status.ACCEPTED).build();
    } else {
      // TODO(gianm): improve error?
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  /**
   * See {@link io.imply.druid.talaria.exec.WorkerClient#postCleanupStage} for the client-side code that calls this API.
   */
  @POST
  @Path("/cleanupStage/{queryId}/{stageNumber}")
  public Response httpPostCleanupStage(
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    worker.postCleanupStage(new StageId(queryId, stageNumber));
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link io.imply.druid.talaria.exec.WorkerClient#postFinish} for the client-side code that calls this API.
   */
  @POST
  @Path("/finish")
  public Response httpPostFinish(@Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    worker.postFinish();
    return Response.status(Response.Status.ACCEPTED).build();
  }


  /**
   * See {@link io.imply.druid.talaria.exec.WorkerClient#getCounters} for the client-side code that calls this API.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/counters")
  public Response httpGetCounters(@Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    return Response.status(Response.Status.OK).entity(worker.getCounters()).build();
  }
}
