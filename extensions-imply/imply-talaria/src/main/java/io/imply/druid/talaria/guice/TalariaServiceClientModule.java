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
import io.imply.druid.talaria.rpc.CoordinatorServiceClient;
import io.imply.druid.talaria.rpc.CoordinatorServiceClientImpl;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.rpc.DiscoveryServiceLocator;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;

import java.util.Collections;
import java.util.List;

public class TalariaServiceClientModule implements DruidModule
{
  private static final int COORDINATOR_ATTEMPTS = 6;

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
  @ManageLifecycle
  @Coordinator
  public ServiceLocator makeCoordinatorServiceLocator(final DruidNodeDiscoveryProvider discoveryProvider)
  {
    return new DiscoveryServiceLocator(discoveryProvider, NodeRole.COORDINATOR);
  }

  @Provides
  public CoordinatorServiceClient makeCoordinatorServiceClient(
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Coordinator final ServiceLocator serviceLocator
  )
  {
    return new CoordinatorServiceClientImpl(
        clientFactory.makeClient(
            NodeRole.COORDINATOR.getJsonName(),
            serviceLocator,
            StandardRetryPolicy.builder().maxAttempts(COORDINATOR_ATTEMPTS).build()
        ),
        jsonMapper
    );
  }
}
