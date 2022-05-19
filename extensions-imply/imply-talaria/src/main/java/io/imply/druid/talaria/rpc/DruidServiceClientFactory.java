/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

/**
 * Factory for creating {@link DruidServiceClient}.
 */
public interface DruidServiceClientFactory
{
  /**
   * Creates a client for a particular service.
   *
   * @param serviceName    name of the service, which is used in log messages and exceptions.
   * @param serviceLocator service locator. This is not owned by the returned client, and should be closed
   *                       separately when you are done with it.
   * @param retryPolicy    retry policy
   */
  DruidServiceClient makeClient(String serviceName, ServiceLocator serviceLocator, RetryPolicy retryPolicy);
}
