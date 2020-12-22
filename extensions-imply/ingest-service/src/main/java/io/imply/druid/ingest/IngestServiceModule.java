/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.files.FileStore;
import io.imply.druid.ingest.files.local.LocalFileStoreModule;
import io.imply.druid.ingest.jobs.JobProcessor;
import io.imply.druid.ingest.jobs.OverlordClient;
import io.imply.druid.ingest.metadata.IngestServiceMetadataStore;
import io.imply.druid.ingest.server.IngestServiceJettyServerInitializer;
import io.imply.druid.ingest.server.JobsResource;
import io.imply.druid.ingest.server.SchemasResource;
import io.imply.druid.ingest.server.TablesResource;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.HttpIndexingServiceClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

public class IngestServiceModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bindConstant().annotatedWith(Names.named("serviceName")).to(IngestService.SERVICE_NAME);
    binder.bindConstant().annotatedWith(Names.named("servicePort")).to(IngestService.PORT);
    binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(IngestService.TLS_PORT);

    JsonConfigProvider.bind(binder, "imply.ingest.tenant", IngestServiceTenantConfig.class);

    PolyBind.createChoice(binder, "imply.ingest.metadata.type", Key.get(IngestServiceMetadataStore.class), null);
    PolyBind.createChoiceWithDefault(
        binder,
        "imply.ingest.files.type",
        Key.get(FileStore.class),
        LocalFileStoreModule.TYPE
    );

    binder.bind(IndexingServiceClient.class).to(HttpIndexingServiceClient.class).in(LazySingleton.class);
    binder.bind(CoordinatorClient.class).in(LazySingleton.class);

    binder.bind(JettyServerInitializer.class).to(IngestServiceJettyServerInitializer.class).in(LazySingleton.class);
    Jerseys.addResource(binder, TablesResource.class);
    Jerseys.addResource(binder, JobsResource.class);
    Jerseys.addResource(binder, SchemasResource.class);

    LifecycleModule.register(binder, Server.class);
    LifecycleModule.register(binder, JobProcessor.class);
  }

  @Provides
  public OverlordClient getOverlordClient(
      IndexingServiceClient indexingServiceClient,
      @Coordinator DruidLeaderClient overlordLeaderClient,
      ObjectMapper jsonMapper
  )
  {
    return new OverlordClient(indexingServiceClient, overlordLeaderClient, jsonMapper);
  }
}
