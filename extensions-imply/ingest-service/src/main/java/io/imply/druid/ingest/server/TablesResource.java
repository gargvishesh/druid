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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import io.imply.druid.ingest.metadata.Table;
import io.imply.druid.ingest.metadata.TableJobStateStats;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
  public Response getTables(
      @DefaultValue("RUNNING") @QueryParam("states") String states,
      @Context final HttpServletRequest req
  )
  {
    Set<TableJobStateStats> allTables;
    Set<JobState> jss = null; // this is the value for "ALL"
    String badArgument = null;
    if (states.toUpperCase(Locale.ROOT).equals("NONE")) {
      jss = Collections.emptySet();
    } else if (!states.toUpperCase(Locale.ROOT).equals("ALL")) {
      // only valid state names are allowed here, note that "ALL" AND "NONE" are not
      // valid here and will be rejected...
      jss = new HashSet<>();
      String[] statesList = states.split(",");
      for (String state : statesList) {
        try {
          jss.add(JobState.valueOf(state.toUpperCase(Locale.ROOT)));
        }
        catch (Exception e) {
          badArgument = state;
        }
      }
    }
    Response response;
    if (badArgument != null) {
      response = Response.status(400).entity(String.format("Bad state name: %s", badArgument)).build();
    } else {
      // all ok....
      allTables = metadataStore.getJobCountPerTablePerState(jss);
      assignPermissions(allTables, req);
      List<TableJobStateStats> sorted = new ArrayList<>(allTables);
      sorted.sort(Comparator.comparing(TableJobStateStats::getName));
      response = Response.ok(ImmutableMap.of("tables", sorted)).build();
    }
    return response;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
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
      // One interesting case if when the table already exists...one approach is just to return
      // a 200 in this case but that may be misleading to the client since some data in the table
      // (i.e. created time) will not be updated. But this maybe good enough since 200 is ok
      // and when we create we return 201 (created).
      // Another caveat is that parsing the
      // exception that we get here is not straightforward since the cause is wrapped inside and the
      // actual exception will depend on the database behind it. It seems more reasonable to
      // create our own exception and manage the exceptions closer to the where the server is called and
      // throw our own exceptions there...
      //
      // For now just saying that all exceptions are errors and
      // propagating the message to the client...
      issue = e.getMessage();
      isOk = false;
    }

    Response r;
    if (isOk) {
      // the table resource is abnormal in the sense that it has no id, the name itself is its own id
      // in a more RESTful way we would append the newly created id to the req.getPathInfo()...
      r = Response.created(URI.create("")).build();
    } else {
      r = Response.serverError().entity(ImmutableMap.of("error", issue)).build();
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
      @PathParam("table")
          String tableName
  )
  {
    if (!metadataStore.druidTableExists(tableName)) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
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
      @PathParam("table")
          String tableName,
      @PathParam("jobId")
          String jobId
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
      @PathParam("table")
          String tableName,
      @PathParam("jobId")
          String jobId
  )
  {
    // this is not enough, we need to handle the alternative where schemaId is set
    int updated = metadataStore.scheduleJob(jobId, scheduleRequest.getSchema());

    if (updated == 0) {
      return Response.status(Response.Status.NOT_FOUND).build();
    } else if (updated > 1) {
      // this should never happen
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
    return Response.created(URI.create("")).build();
  }

  private void assignPermissions(Set<TableJobStateStats> tables, HttpServletRequest req)
  {
    // decorate WRITE tables:
    // iterate through list of tables, filter by write authorized
    Function<Table, Iterable<ResourceAction>> raGenerator = table -> Collections.singletonList(
        new ResourceAction(new Resource(table.getName(), ResourceType.DATASOURCE), Action.WRITE)
    );
    Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            tables,
            raGenerator,
            authorizerMapper
        )
    ).forEach(ts -> ts.addPermissions(Action.WRITE));

    // another pass with Action.READ
    // Need to reset the checked flag...
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, null);
    raGenerator = table -> Collections.singletonList(
        new ResourceAction(new Resource(table.getName(), ResourceType.DATASOURCE), Action.READ)
    );
    Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            tables,
            raGenerator,
            authorizerMapper
        )
    ).forEach(ts -> ts.addPermissions(Action.READ));
  }
}
