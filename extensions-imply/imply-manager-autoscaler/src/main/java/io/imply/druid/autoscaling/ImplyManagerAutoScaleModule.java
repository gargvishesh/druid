/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.imply.druid.autoscaling.client.ImplyManagerServiceClient;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.http.client.HttpClient;

import java.util.Collections;
import java.util.List;

public class ImplyManagerAutoScaleModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(new SimpleModule("ImplyManagerAutoScaleModule").registerSubtypes(
        ImplyManagerAutoScaler.class));
  }

  @Override
  public void configure(Binder binder)
  {
  }

  @Provides
  public ImplyManagerServiceClient getImplyManagerServiceClient(
      @EscalatedGlobal HttpClient httpClient,
      @Json ObjectMapper jsonMapper
  )
  {
    return new ImplyManagerServiceClient(httpClient, jsonMapper);
  }
}

