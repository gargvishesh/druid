/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.google.common.base.Function;
import com.google.inject.Inject;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.metadata.IngestJob;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Path("/ingest/v1/jobs")
public class JobsResource
{
  private final AuthorizerMapper authorizerMapper;
  private final IngestServiceMetadataStore metadataStore;

  @Inject
  public JobsResource(
      AuthorizerMapper authorizerMapper,
      IngestServiceMetadataStore metadataStore
  )
  {
    this.authorizerMapper = authorizerMapper;
    this.metadataStore = metadataStore;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllJobs(
      @Context final HttpServletRequest req,
      @QueryParam("jobState") @Nullable String jobState
  )
  {
    // iterate through list of jobs, filter by read authorized on associated table name
    Function<IngestJob, Iterable<ResourceAction>> raGenerator = job -> Collections.singletonList(
        new ResourceAction(new Resource(job.getTableName(), ResourceType.DATASOURCE), Action.READ)
    );

    JobState targetState;
    try {
      targetState = JobState.fromString(jobState);
    }
    catch (IllegalArgumentException iae) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    List<IngestJob> jobsFromMetadata = metadataStore.getJobs(targetState);
    List<IngestJobInfo> jobs = StreamSupport.stream(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            jobsFromMetadata,
            raGenerator,
            authorizerMapper
        ).spliterator(),
        false
    ).map(IngestJobInfo::fromIngestJob).collect(Collectors.toList());

    return Response.ok(new IngestJobsResponse(jobs)).build();
  }

  @GET
  @Path("/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getJob(
      @Context final HttpServletRequest req,
      @PathParam("jobId") String jobId
  )
  {
    // this is not enough, we need to handle the alternative where schemaId is set
    IngestJob job = metadataStore.getJob(jobId);
    if (job == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    Access authResult = AuthorizationUtils.authorizeResourceAction(
        req,
        new ResourceAction(
            new Resource(job.getTableName(), ResourceType.DATASOURCE),
            Action.READ
        ),
        authorizerMapper
    );

    if (!authResult.isAllowed()) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }

    return Response.ok(IngestJobInfo.fromIngestJob(job)).build();
  }
}
