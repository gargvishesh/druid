/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import org.apache.druid.java.util.http.client.HttpClient;

import java.util.concurrent.ScheduledExecutorService;

public class DruidServiceClientFactoryImpl implements DruidServiceClientFactory
{
  private final HttpClient httpClient;
  private final ScheduledExecutorService connectExec;

  public DruidServiceClientFactoryImpl(
      final HttpClient httpClient,
      final ScheduledExecutorService connectExec
  )
  {
    this.httpClient = httpClient;
    this.connectExec = connectExec;
  }

  @Override
  public DruidServiceClient makeClient(
      final String serviceName,
      final ServiceLocator serviceLocator,
      final RetryPolicy retryPolicy
  )
  {
    return new DruidServiceClientImpl(serviceName, httpClient, serviceLocator, retryPolicy, connectExec);
  }
}
