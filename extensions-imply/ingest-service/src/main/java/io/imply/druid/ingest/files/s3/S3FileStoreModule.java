/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.files.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.imply.druid.ingest.files.FileStore;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Collections;
import java.util.List;

public class S3FileStoreModule implements DruidModule
{
  public static final String TYPE = "s3";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(
        binder,
        StringUtils.format("%s.%s", FileStore.STORE_PROPERTY_BASE, TYPE),
        S3FileStoreConfig.class
    );

    PolyBind.optionBinder(binder, Key.get(FileStore.class))
            .addBinding(TYPE)
            .to(S3FileStore.class)
            .in(LazySingleton.class);
  }

  @Provides
  public AmazonS3 getS3Client(
      AWSCredentialsProvider provider,
      AWSClientConfig clientConfig
  )
  {
    final ClientConfiguration configuration = new ClientConfigurationFactory().getConfig();
    final AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3Client
        .builder()
        .withCredentials(provider)
        .withClientConfiguration(configuration)
        .withChunkedEncodingDisabled(clientConfig.isDisableChunkedEncoding())
        .withPathStyleAccessEnabled(clientConfig.isEnablePathStyleAccess())
        .withForceGlobalBucketAccessEnabled(clientConfig.isForceGlobalBucketAccessEnabled());

    return amazonS3ClientBuilder.build();
  }
}
