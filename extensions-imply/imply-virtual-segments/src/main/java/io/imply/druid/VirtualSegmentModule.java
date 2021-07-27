/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid;

import com.fasterxml.jackson.databind.Module;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.imply.druid.loading.FIFOSegmentReplacementStrategy;
import io.imply.druid.loading.SegmentReplacementStrategy;
import io.imply.druid.loading.VirtualSegmentCacheManager;
import io.imply.druid.loading.VirtualSegmentLoader;
import io.imply.druid.processing.Deferred;
import io.imply.druid.query.DeferredQueryProcessingPool;
import io.imply.druid.segment.VirtualSegmentStateManager;
import io.imply.druid.segment.VirtualSegmentStateManagerImpl;
import io.imply.druid.server.DeferredLoadingQuerySegmentWalker;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoader;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * This module can only be loaded on historicals.
 */
public class VirtualSegmentModule implements DruidModule
{
  public static final String PROPERTY_PREFIX = "druid.virtualSegment";
  public static final String ENABLED_PROPERTY = PROPERTY_PREFIX + ".enabled";

  @Inject
  private Properties props;

  public VirtualSegmentModule()
  {

  }

  @VisibleForTesting
  VirtualSegmentModule(Properties properties)
  {
    this.props = properties;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    if (!isEnabled()) {
      return;
    }
    JsonConfigProvider.bind(binder, PROPERTY_PREFIX, VirtualSegmentConfig.class);

    binder.bind(SegmentLoader.class).to(VirtualSegmentLoader.class).in(LazySingleton.class);
    binder.bind(SegmentCacheManager.class).to(VirtualSegmentCacheManager.class).in(LazySingleton.class);
    binder.bind(VirtualSegmentStateManager.class).to(VirtualSegmentStateManagerImpl.class).in(LazySingleton.class);
    binder.bind(SegmentReplacementStrategy.class).to(FIFOSegmentReplacementStrategy.class).in(LazySingleton.class);

    // Query bindings
    binder.bind(QuerySegmentWalker.class).to(DeferredLoadingQuerySegmentWalker.class).in(LazySingleton.class);
    binder.bind(QueryProcessingPool.class).annotatedWith(Deferred.class).to(DeferredQueryProcessingPool.class).in(LazySingleton.class);
  }

  private boolean isEnabled()
  {
    Preconditions.checkNotNull(props, "props field was not injected");
    return Boolean.valueOf(props.getProperty(ENABLED_PROPERTY, "false"));
  }
}
