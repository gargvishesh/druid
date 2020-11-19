/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.servlet.GuiceFilter;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitUtils;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.initialization.jetty.LimitRequestsFilter;
import org.apache.druid.server.security.AuthenticationUtils;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.List;
import java.util.Properties;

public class IngestServiceJettyServerInitializer implements JettyServerInitializer
{
  private static final Logger LOG = new Logger(IngestServiceJettyServerInitializer.class);

  private static List<String> UNSECURED_PATHS = ImmutableList.of(
      "/status/health",
      "/ingest/isLeader"
  );


  private final IngestServiceTenantConfig config;
  private final ServerConfig serverConfig;

  @Inject
  public IngestServiceJettyServerInitializer(
      IngestServiceTenantConfig config,
      Properties properties,
      ServerConfig serverConfig
  )
  {
    this.config = config;
    this.serverConfig = serverConfig;
  }


  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    // Add LimitRequestsFilter as first in the chain if enabled.
    if (serverConfig.isEnableRequestLimit()) {
      //To reject xth request, limit should be set to x-1 because (x+1)st request wouldn't reach filter
      // but rather wait on jetty queue.
      Preconditions.checkArgument(
          serverConfig.getNumThreads() > 1,
          "numThreads must be > 1 to enable Request Limit Filter."
      );
      LOG.info("Enabling Request Limit Filter with limit [%d].", serverConfig.getNumThreads() - 1);
      root.addFilter(new FilterHolder(new LimitRequestsFilter(serverConfig.getNumThreads() - 1)),
                     "/*", null
      );
    }

    final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

    // perform no-op authorization for these resources
    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, UNSECURED_PATHS);

    List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
    AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

    JettyServerInitUtils.addAllowHttpMethodsFilter(root, serverConfig.getAllowedHttpMethods());

    JettyServerInitUtils.addExtensionFilters(root, injector);

    // Check that requests were authorized before sending responses
    AuthenticationUtils.addPreResponseAuthorizationCheckFilter(
        root,
        authenticators,
        jsonMapper
    );

    root.addFilter(GuiceFilter.class, "/*", null);

    final HandlerList handlerList = new HandlerList();
    // Do not change the order of the handlers that have already been added
    for (Handler handler : server.getHandlers()) {
      handlerList.addHandler(handler);
    }

    handlerList.addHandler(JettyServerInitUtils.getJettyRequestLogHandler());

    // Add Gzip handler at the very end
    handlerList.addHandler(
        JettyServerInitUtils.wrapWithDefaultGzipHandler(
            root,
            serverConfig.getInflateBufferSize(),
            serverConfig.getCompressionLevel()
        )
    );

    final StatisticsHandler statisticsHandler = new StatisticsHandler();
    statisticsHandler.setHandler(handlerList);

    server.setHandler(statisticsHandler);
  }
}
