/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.imply.druid.talaria.rpc.DiscoveryServiceLocator;
import io.imply.druid.talaria.rpc.DruidServiceClientFactory;
import io.imply.druid.talaria.rpc.DruidServiceClientFactoryImpl;
import io.imply.druid.talaria.rpc.ServiceLocator;
import io.imply.druid.talaria.rpc.StandardRetryPolicy;
import io.imply.druid.talaria.rpc.indexing.OverlordServiceClient;
import io.imply.druid.talaria.rpc.indexing.OverlordServiceClientImpl;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.http.client.HttpClient;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public class TalariaServiceClientModule implements DruidModule
{
  private static final int CONNECT_EXEC_THREADS = 4;
  private static final int OVERLORD_ATTEMPTS = 3;

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    // Nothing to do.
  }

  @Provides
  @LazySingleton
  @EscalatedGlobal
  public DruidServiceClientFactory makeServiceClientFactory(@EscalatedGlobal final HttpClient httpClient)
  {
    final ScheduledExecutorService connectExec =
        ScheduledExecutors.fixed(CONNECT_EXEC_THREADS, "DruidServiceClientFactory-%d");
    return new DruidServiceClientFactoryImpl(httpClient, connectExec);
  }

  @Provides
  @ManageLifecycle
  @IndexingService
  public ServiceLocator makeOverlordServiceLocator(final DruidNodeDiscoveryProvider discoveryProvider)
  {
    return new DiscoveryServiceLocator(discoveryProvider, NodeRole.OVERLORD);
  }

  @Provides
  public OverlordServiceClient makeOverlordServiceClient(
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final DruidServiceClientFactory clientFactory,
      @IndexingService final ServiceLocator serviceLocator
  )
  {
    return new OverlordServiceClientImpl(
        clientFactory.makeClient(
            NodeRole.OVERLORD.getJsonName(),
            serviceLocator,
            new StandardRetryPolicy(OVERLORD_ATTEMPTS)
        ),
        jsonMapper
    );
  }
}
