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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Collections;
import java.util.List;

@Path("/ingest/v1/tables")
public class TablesResource
{
  private final IngestServiceTenantConfig config;
  private final AuthorizerMapper authorizerMapper;
  private final FileStore fileStore;
  private final IngestServiceMetadataStore metadataStore;

  @Inject
  public TablesResource(
      IngestServiceTenantConfig config,
      AuthorizerMapper authorizerMapper,
      FileStore fileStore,
      IngestServiceMetadataStore metadataStore
  )
  {
    this.config = config;
    this.authorizerMapper = authorizerMapper;
    this.fileStore = fileStore;
    this.metadataStore = metadataStore;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllTables(
      @Context final HttpServletRequest req
  )
  {
    // iterate through list of tables, filter by write authorized
    Function<String, Iterable<ResourceAction>> raGenerator = table -> Collections.singletonList(
        new ResourceAction(new Resource(table, ResourceType.DATASOURCE), Action.WRITE)
    );


    List<String> allTables = metadataStore.getAllTableNames();

    List<String> authorizedTables = Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            allTables,
            raGenerator,
            authorizerMapper
        )
    );
    return Response.ok(ImmutableMap.of("tables", authorizedTables)).build();
  }

  @POST
  @Path("/{table}")
  public Response createTable(
      @PathParam("table") String tableName,
      @Context final HttpServletRequest req
  )
  {
    // does this just need to accept a table name? or JSON request to potentially provide other table level details?
    // this needs to handle authorization differently since it maybe requires some sort of create table permissions...
    // however just use table write for now...
    AuthorizationUtils.authorizeResourceAction(
        req,
        new ResourceAction(new Resource(tableName, ResourceType.DATASOURCE), Action.WRITE),
        authorizerMapper
    );
    boolean isOk = true;
    String issue = null;
    try {
      metadataStore.insertTable(tableName);
    }
    catch (Exception e) {
      // TODO: we should be more fine grained here with respect to what happened...
      issue = e.getMessage();
      isOk = false;
    }

    Response r;
    if (isOk) {
      r = Response.ok().build();
    } else {
      r = Response.serverError().entity(issue).build();
    }
    return r;
  }

  @DELETE
  @Path("/{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TablesResourceFilter.class)
  public Response dropTable(
      @PathParam("table") String tableName
  )
  {
    return Response.noContent().build();
  }

  @DELETE
  @Path("/{table}/data")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TablesResourceFilter.class)
  public Response deleteData(
      @PathParam("table") String tableName,
      @QueryParam("startTimestamp") String startTimestamp,
      @QueryParam("endTimestamp") String endTimestamp
  )
  {
    return Response.noContent().build();
  }

  @GET
  @Path("/{table}/jobs")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TablesResourceFilter.class)
  public Response listIngestJobs(
      @PathParam("table") String tableName
  )
  {
    return Response.noContent().build();
  }

  @POST
  @Path("/{table}/jobs")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TablesResourceFilter.class)
  public Response stageIngestJob(
      final JobRunner jobType,
      @PathParam("table") String tableName
  )
  {
    Preconditions.checkArgument(metadataStore.druidTableExists(tableName));
    // make jobId uuid, generate URL for file dropoff
    // write staging job to somewhere with 'staged' state
    final JobRunner jobTypeToUse = jobType != null ? jobType : new BatchAppendJobRunner();
    String jobId = metadataStore.stageJob(tableName, jobTypeToUse);
    URI dropoff = fileStore.makeDropoffUri(jobId);
    return Response.ok(ImmutableMap.of("jobId", jobId, "dropoffUri", dropoff)).build();
  }

  @DELETE
  @Path("/{table}/jobs/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TablesResourceFilter.class)
  public Response cancelIngestJob(
      @PathParam("table") String tableName,
      @PathParam("jobId") String jobId
  )
  {
    // cancel job
    return Response.noContent().build();
  }

  @POST
  @Path("/{table}/jobs/{jobId}/sample")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TablesResourceFilter.class)
  public Response sampleIngestJob(
      final IngestJobRequest sampleRequest,
      @PathParam("table") String tableName,
      @PathParam("jobId") String jobId
  )
  {
    // most of this logic should probably live somewhere else, but;
    // check for existing sample file in sample store
    // - if exists, issue sample request to overlord using sample file as inline input source
    // - if not, issue sample request to overlord using actual file as s3 input source, and save sampled output into
    //   new sample file
    return Response.noContent().build();
  }

  @POST
  @Path("/{table}/jobs/{jobId}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TablesResourceFilter.class)
  public Response scheduleIngestJob(
      final IngestJobRequest scheduleRequest,
      @PathParam("table") String tableName,
      @PathParam("jobId") String jobId
  )
  {
    // this is not enough, we need to handle the alternative where schemaId is set
    metadataStore.scheduleJob(jobId, scheduleRequest.getSchema());
    return Response.noContent().build();
  }
}
