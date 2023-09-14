/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.imply.clarity.emitter.BaseClarityEmitterConfig;
import io.imply.clarity.emitter.ClarityNodeDetails;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.server.DruidNode;
import org.apache.druid.utils.JvmUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ClarityHttpEmitterModule implements DruidModule
{
  static final String PROPERTY_JFR_PROFILER = "druid.emitter.clarity.jfrProfiler";

  private static final Logger log = new Logger(ClarityHttpEmitterModule.class);
  private static final String EMITTER_TYPE = "clarity";

  private Properties properties;

  @Inject
  public void setProperties(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.clarity", ClarityHttpEmitterConfig.class);
    JsonConfigProvider.bind(binder, "druid.emitter.clarity.ssl", ClarityHttpEmitterSSLClientConfig.class);
    binder.bind(BaseClarityEmitterConfig.class).to(ClarityHttpEmitterConfig.class);
    binder.bind(JdkVersion.class).toProvider(JdkVersionProvider.class).in(LazySingleton.class);

    if (Boolean.parseBoolean(properties.getProperty(PROPERTY_JFR_PROFILER, "false"))) {
      binder.bind(JfrProfilerManager.class).in(ManageLifecycle.class);
      LifecycleModule.register(binder, JfrProfilerManager.class);
    }
  }

  @Provides
  @ManageLifecycle
  @Named(EMITTER_TYPE)
  public Emitter getEmitter(
      Supplier<ClarityHttpEmitterConfig> configSupplier,
      Supplier<ClarityHttpEmitterSSLClientConfig> sslConfig,
      Lifecycle lifecycle,
      @Json ObjectMapper jsonMapper,
      Injector injector
  )
  {
    final ClarityHttpEmitterConfig config = configSupplier.get();
    HttpClientConfig httpClientConfig = getHttpClientConfig(configSupplier.get(), sslConfig.get());
    return new ClarityHttpEmitter(
        config,
        ClarityNodeDetails.fromInjector(injector, config),
        HttpClientInit.createClient(httpClientConfig, lifecycle),
        jsonMapper
    );
  }

  @Provides
  public JfrProfilerManagerConfig getJfrProfilerManagerConfig(
      ClarityHttpEmitterConfig config,
      @Self DruidNode node,
      Properties properties
  )
  {
    JfrProfilerManagerConfig.Builder builder = new JfrProfilerManagerConfig.Builder();
    Map<String, String> autoTags = new HashMap<>();

    autoTags.put("host", node.getHostAndPortToUse());
    autoTags.put("service", node.getServiceName());
    addIfExists(autoTags, properties, "dataSource", "druid.metrics.emitter.dimension.dataSource");
    addIfExists(autoTags, properties, "taskId", "druid.metrics.emitter.dimension.taskId");
    addIfExists(autoTags, properties, "taskType", "druid.metrics.emitter.dimension.taskType");
    addIfExists(autoTags, properties, "implyCluster", "druid.emitter.clarity.clusterName");
    builder.addTags(autoTags);
    builder.addTags(config.getJfrProfilerTags());

    return builder.build();
  }

  private void addIfExists(Map<String, String> map, Properties properties, String key, String propertyKey)
  {
    if (properties.containsKey(propertyKey)) {
      map.put(key, properties.getProperty(propertyKey));
    }
  }

  @VisibleForTesting
  public static HttpClientConfig getHttpClientConfig(
      ClarityHttpEmitterConfig config,
      ClarityHttpEmitterSSLClientConfig sslConfig
  )
  {
    final HttpClientConfig.Builder builder = HttpClientConfig
        .builder()
        .withNumConnections(1)
        .withReadTimeout(config.getReadTimeout().toStandardDuration())
        .withHttpProxyConfig(config.getProxyConfig());

    if (config.getWorkerCount() != null) {
      builder.withWorkerCount(config.getWorkerCount());
    } else {
      builder.withWorkerCount(getDefaultWorkerCount());
    }

    final SSLContext context;
    if (sslConfig.getTrustStorePath() == null) {
      try {
        context = SSLContext.getDefault();
      }
      catch (NoSuchAlgorithmException e) {
        throw Throwables.propagate(e);
      }
    } else {
      context = getSSLContext(sslConfig);
    }
    builder.withSslContext(context);
    return builder.build();
  }

  @VisibleForTesting
  public static int getDefaultWorkerCount()
  {
    return Math.min(10, JvmUtils.getRuntimeInfo().getAvailableProcessors() * 2);
  }

  private static SSLContext getSSLContext(ClarityHttpEmitterSSLClientConfig config)
  {
    log.info("Creating SslContext for https client using config [%s]", config);

    SSLContext sslContext = null;
    try {
      sslContext = SSLContext.getInstance(config.getProtocol() == null ? "TLSv1.2" : config.getProtocol());
      KeyStore keyStore = KeyStore.getInstance(config.getTrustStoreType() == null
                                               ? KeyStore.getDefaultType()
                                               : config.getTrustStoreType());
      keyStore.load(
          new FileInputStream(config.getTrustStorePath()),
          config.getTrustStorePasswordProvider().getPassword().toCharArray()
      );
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(config.getTrustStoreAlgorithm() == null
                                                                                ? TrustManagerFactory.getDefaultAlgorithm()
                                                                                : config.getTrustStoreAlgorithm());
      trustManagerFactory.init(keyStore);
      sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
    }
    catch (CertificateException | KeyManagementException | IOException | KeyStoreException |
           NoSuchAlgorithmException e) {
      Throwables.propagate(e);
    }
    return sslContext;
  }
}
