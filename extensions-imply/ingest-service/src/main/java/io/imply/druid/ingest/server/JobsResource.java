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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
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
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

@Path("/ingest/v1/jobs")
public class JobsResource
{
  private final IngestServiceTenantConfig config;
  private final AuthorizerMapper authorizerMapper;
  private final IngestServiceMetadataStore metadataStore;

  @Inject
  public JobsResource(
      IngestServiceTenantConfig config,
      AuthorizerMapper authorizerMapper,
      IngestServiceMetadataStore metadataStore
  )
  {
    this.config = config;
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
    // iterate through list of tables, filter by write authorized
    Function<String, Iterable<ResourceAction>> raGenerator = table -> Collections.singletonList(
        new ResourceAction(new Resource(table, ResourceType.DATASOURCE), Action.WRITE)
    );

    // todo: get list of all jobs here
    List<String> allTables = ImmutableList.of("somejob", "someotherjob");

    List<String> authorizedTables = Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            allTables,
            raGenerator,
            authorizerMapper
        )
    );
    return Response.ok(authorizedTables).build();
  }
}
