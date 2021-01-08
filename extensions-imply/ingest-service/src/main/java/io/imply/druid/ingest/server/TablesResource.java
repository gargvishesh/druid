/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.jobs.JobRunner;
import io.imply.druid.ingest.jobs.JobState;
import io.imply.druid.ingest.jobs.OverlordClient;
import io.imply.druid.ingest.jobs.runners.BatchAppendJobRunner;
import io.imply.druid.ingest.metadata.IngestSchema;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import io.imply.druid.ingest.metadata.JobScheduleException;
import io.imply.druid.ingest.metadata.Table;
import io.imply.druid.ingest.samples.SampleStore;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.overlord.sampler.IndexTaskSamplerSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
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
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Path("/ingest/v1/tables")
public class TablesResource
{
  private static final Logger LOG = new Logger(TablesResource.class);

  private static final Function<Table, Iterable<ResourceAction>> TABLE_WRITE_RA_GENERATOR =
      table -> Collections.singletonList(
          new ResourceAction(new Resource(table.getName(), ResourceType.DATASOURCE), Action.WRITE)
      );
  private static final Function<Table, Iterable<ResourceAction>> TABLE_READ_RA_GENERATOR =
      table -> Collections.singletonList(
          new ResourceAction(new Resource(table.getName(), ResourceType.DATASOURCE), Action.READ)
      );

  private final IngestServiceTenantConfig config;
  private final AuthorizerMapper authorizerMapper;
  private final FileStore fileStore;
  private final IngestServiceMetadataStore metadataStore;
  private final SampleStore sampleStore;
  private final OverlordClient overlordClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public TablesResource(
      IngestServiceTenantConfig config,
      AuthorizerMapper authorizerMapper,
      FileStore fileStore,
      IngestServiceMetadataStore metadataStore,
      SampleStore sampleStore,
      OverlordClient overlordClient,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.authorizerMapper = authorizerMapper;
    this.fileStore = fileStore;
    this.metadataStore = metadataStore;
    this.sampleStore = sampleStore;
    this.overlordClient = overlordClient;
    this.jsonMapper = jsonMapper;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTables(
      @DefaultValue("ALL") @QueryParam("states") String states,
      @Context final HttpServletRequest req
  )
  {
    Set<JobState> jobStateFilter = null; // this is the value for "ALL"
    String badArgument = null;
    if ("NONE".equals(StringUtils.toUpperCase(states))) {
      jobStateFilter = Collections.emptySet();
    } else if (!"ALL".equals(StringUtils.toUpperCase(states))) {
      // only valid state names are allowed here, note that "ALL" AND "NONE" are not
      // valid here and will be rejected...
      jobStateFilter = new HashSet<>();
      String[] statesList = states.split(",");
      for (String state : statesList) {
        try {
          jobStateFilter.add(JobState.valueOf(StringUtils.toUpperCase(state)));
        }
        catch (Exception e) {
          badArgument = state;
        }
      }
    }
    Response response;
    if (badArgument != null) {
      response = badRequest(StringUtils.format("Bad state name: %s", badArgument));
    } else {
      List<TableInfo> tables = getTablesInfo(req, jobStateFilter);
      response = Response.ok(new TablesResponse(tables)).build();
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

  @POST
  @Path("/{table}/jobs")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TablesResourceFilter.class)
  public Response stageIngestJob(
      final JobRunner jobType,
      @PathParam("table") String tableName
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
    if (Strings.isNullOrEmpty(tableName)) {
      return Response.status(Response.Status.BAD_REQUEST).entity("table name cannot be empty or null").build();
    }

    if (Strings.isNullOrEmpty(jobId)) {
      return Response.status(Response.Status.BAD_REQUEST).entity("jobId cannot be empty or null").build();
    }

    // check for existing sample file in sample store
    // - if exists, issue sample request to overlord using sample file as inline input source
    // - if not, issue sample request to overlord using actual file as s3 input source, and save sampled output into
    //   new sample file
    final SamplerSpec samplerSpec;
    try {
      samplerSpec = buildSamplerSpec(sampleRequest, tableName, jobId);
    }
    catch (IOException ioe) {
      // IOException means there was an issue reading from the sample store, our fault probably
      LOG.error("Encountered IOException while reading from sample store for jobId[%s], table[%s]", jobId, tableName);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
    catch (Exception ex) {
      // Other exceptions are probably from user error, such as a malformed request
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    try {
      SamplerResponse samplerResponse = overlordClient.sample(samplerSpec);
      sampleStore.storeSample(jobId, samplerResponse);
      return Response.ok(samplerResponse).build();
    }
    catch (Exception ex) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
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
    final String jobTable = metadataStore.getJobTable(jobId);
    if (!tableName.equals(jobTable)) {
      return badRequest(StringUtils.format("job [%s] does not belong to table [%s]", jobId, tableName));
    }
    int updated;
    try {
      if (scheduleRequest.getSchema() != null) {
        updated = metadataStore.scheduleJob(jobId, scheduleRequest.getSchema());
      } else if (scheduleRequest.getSchemaId() != null) {
        if (!metadataStore.schemaExists(scheduleRequest.getSchemaId())) {
          return badRequest(
              StringUtils.format(
                  "schema [%s] does not exist, cannot schedule job [%s]",
                  scheduleRequest.getSchemaId(),
                  jobId
              )
          );
        }
        updated = metadataStore.scheduleJob(jobId, scheduleRequest.getSchemaId());
      } else {
        return badRequest(StringUtils.format("must specify 'schema' or 'schemaId' to schedule job [%s]", jobId));
      }
    }
    catch (JobScheduleException jex) {
      return badRequest(jex.getMessage());
    }

    if (updated == 0) {
      return Response.status(Response.Status.NOT_FOUND).build();
    } else if (updated > 1) {
      // this should never happen
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
    return Response.created(URI.create("")).build();
  }

  private Response badRequest(String error)
  {
    return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("error", error)).build();
  }

  private List<TableInfo> getTablesInfo(HttpServletRequest req, Set<JobState> jobStateFilter)
  {
    final Map<Table, Object2IntMap<JobState>> tableJobSummaries = metadataStore.getTableJobSummary(jobStateFilter);
    final Set<String> writeTables = StreamSupport.stream(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            tableJobSummaries.keySet(),
            TABLE_WRITE_RA_GENERATOR,
            authorizerMapper
        ).spliterator(),
        false
    ).map(Table::getName).collect(Collectors.toSet());
    // re-use auth result for 2nd pass so it doesn't get set twice
    final AuthenticationResult authResult = AuthorizationUtils.authenticationResultFromRequest(req);
    final Set<String> readTables = StreamSupport.stream(
        AuthorizationUtils.filterAuthorizedResources(
            authResult,
            tableJobSummaries.keySet(),
            TABLE_READ_RA_GENERATOR,
            authorizerMapper
        ).spliterator(),
        false
    ).map(Table::getName).collect(Collectors.toSet());

    return tableJobSummaries.entrySet()
                        .stream()
                        .map(kvp -> {
                          final Set<Action> actions = new HashSet<>();
                          final Table t = kvp.getKey();
                          if (readTables.contains(t.getName())) {
                            actions.add(Action.READ);
                          }
                          if (writeTables.contains(t.getName())) {
                            actions.add(Action.WRITE);
                          }
                          final List<TableInfo.JobStateCount> jobStateCounts;
                          if (kvp.getValue() != null) {
                            jobStateCounts = kvp.getValue()
                                                .object2IntEntrySet()
                                                .stream()
                                                .map(e -> new TableInfo.JobStateCount(e.getKey(), e.getIntValue()))
                                                .collect(Collectors.toList());
                          } else {
                            jobStateCounts = null;
                          }
                          return new TableInfo(
                              t.getName(),
                              t.getCreatedTime(),
                              actions.size() > 0 ? actions : null,
                              jobStateCounts
                          );
                        })
                        .sorted(Comparator.comparing(TableInfo::getName))
                        .collect(Collectors.toList());
  }

  protected SamplerSpec buildSamplerSpec(
      IngestJobRequest jobRequest,
      String tableName,
      String jobId
  ) throws IOException
  {
    final IngestSchema ingestSchema = jobRequest.getSchema();
    final DataSchema dataSchema = new DataSchema(
        tableName,
        ingestSchema.getTimestampSpec(),
        ingestSchema.getDimensionsSpec(),
        new AggregatorFactory[]{}, // no rollup for now
        new UniformGranularitySpec(
            ingestSchema.getPartitionScheme().getSegmentGranularity(),
            Granularities.NONE,
            false, // no rollup for now
            null
        ),
        null
    );

    // if we've already sampled before, use the cached response data from the previous sampling
    // otherwise, use the input file from the file store
    final SamplerResponse cachedSamplerResponse = sampleStore.getSamplerResponse(jobId);
    final InputSource inputSource = cachedSamplerResponse != null ?
                                    makeInlineInputSource(cachedSamplerResponse) :
                                    fileStore.makeInputSource(jobId);

    final IndexTask.IndexIOConfig ioConfig = new IndexTask.IndexIOConfig(
        null,
        inputSource,
        ingestSchema.getInputFormat(),
        null
    );

    return new IndexTaskSamplerSpec(
        new IndexTask.IndexIngestionSpec(
            dataSchema,
            ioConfig,
            null
        ),
        null,
        null
    );
  }

  private InlineInputSource makeInlineInputSource(SamplerResponse cachedSamplerResponse)
  {
    try {
      return new InlineInputSource(jsonMapper.writeValueAsString(cachedSamplerResponse.getData()));
    }
    catch (JsonProcessingException jpe) {
      throw new RuntimeException(jpe);
    }
  }
}
