/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.imply.druid.ingest.client.IngestServiceClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class IngestServiceSchemaModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(IngestServiceSchema.class).in(Scopes.SINGLETON);
    SqlBindings.addSchema(binder, NamedIngestServiceSchema.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Provides
  public IngestServiceClient getIngestServiceClient(
      @EscalatedGlobal HttpClient httpClient,
      DruidNodeDiscoveryProvider discoveryProvider,
      @Json ObjectMapper jsonMapper
  )
  {
    return new IngestServiceClient(httpClient, discoveryProvider, jsonMapper);
  }
}
