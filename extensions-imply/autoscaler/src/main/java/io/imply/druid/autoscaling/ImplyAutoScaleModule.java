/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling;

import io.imply.druid.autoscaling.server.ImplyManagerServiceClient;
import sun.net.www.http.HttpClient;

import javax.xml.bind.Binder;
import java.util.Collections;

public class ImplyAutoScaleModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(new SimpleModule("ImplyAutoScaleModule").registerSubtypes(ImplyAutoScaler.class));
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

