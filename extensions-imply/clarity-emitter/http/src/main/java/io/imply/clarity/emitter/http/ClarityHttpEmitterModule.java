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
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.imply.clarity.emitter.BaseClarityEmitterConfig;
import io.imply.clarity.emitter.ClarityNodeDetails;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;

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
import java.util.List;

public class ClarityHttpEmitterModule implements DruidModule
{
  private static final Logger log = new Logger(ClarityHttpEmitterModule.class);
  private static final String EMITTER_TYPE = "clarity";

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
    final HttpClientConfig.Builder builder = HttpClientConfig
        .builder()
        .withNumConnections(1)
        .withReadTimeout(config.getReadTimeout().toStandardDuration())
        .withHttpProxyConfig(config.getProxyConfig());

    final SSLContext context;
    if (sslConfig.get().getTrustStorePath() == null) {
      try {
        context = SSLContext.getDefault();
      }
      catch (NoSuchAlgorithmException e) {
        throw Throwables.propagate(e);
      }
    } else {
      context = getSSLContext(sslConfig.get());
    }
    builder.withSslContext(context);

    return new ClarityHttpEmitter(
        config,
        ClarityNodeDetails.fromInjector(injector, config),
        HttpClientInit.createClient(builder.build(), lifecycle),
        jsonMapper
    );
  }

  private SSLContext getSSLContext(ClarityHttpEmitterSSLClientConfig config)
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
    catch (CertificateException | KeyManagementException | IOException | KeyStoreException | NoSuchAlgorithmException e) {
      Throwables.propagate(e);
    }
    return sslContext;
  }
}
