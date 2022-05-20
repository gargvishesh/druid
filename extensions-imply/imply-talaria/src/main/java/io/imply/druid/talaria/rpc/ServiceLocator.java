/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;

/**
 * Used by {@link DruidServiceClient} to locate services. Thread-safe.
 */
public interface ServiceLocator extends Closeable
{
  /**
   * Returns a future that resolves to a set of {@link ServiceLocation}.
   *
   * If the returned object returns true from {@link ServiceLocations#isClosed()}, it means the service has closed
   * permanently. Otherwise, any of the returned locations in {@link ServiceLocations#getLocations()} is a viable
   * selection.
   *
   * It is possible for the list of locations to be empty. This means that the service is not currently available,
   * but also has not been closed, so it may become available at some point in the future.
   */
  ListenableFuture<ServiceLocations> locate();

  @Override
  void close();
}
