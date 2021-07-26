/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.server.coordination.ServerManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Properties;

public class VirtualSegmentModuleTest
{
  private static ServerManager mockServerManager = Mockito.mock(ServerManager.class);
  private static SegmentLoader mockSegmentLoader = Mockito.mock(SegmentLoader.class);
  private static SegmentCacheManager mockCacheManager = Mockito.mock(SegmentCacheManager.class);

  @Test
  public void testGetSegmentLoader_VirtualConfigDisabled()
  {
    Injector injector = Guice.createInjector(Modules.override(new BaseModule()).with(new VirtualSegmentModule(new Properties())));
    Assert.assertEquals(mockSegmentLoader, injector.getInstance(SegmentLoader.class));
  }

  @Test
  public void testGetQuerySegmentWalker_VirtualConfigDisabled()
  {
    Injector injector = Guice.createInjector(Modules.override(new BaseModule()).with(new VirtualSegmentModule(new Properties())));
    Assert.assertEquals(mockServerManager, injector.getInstance(QuerySegmentWalker.class));
  }

  @Test
  public void testGetSegmentCacheManager_VirtualConfigDisabled()
  {
    Injector injector = Guice.createInjector(Modules.override(new BaseModule()).with(new VirtualSegmentModule(new Properties())));
    Assert.assertEquals(mockCacheManager, injector.getInstance(SegmentCacheManager.class));
  }

  private static class BaseModule extends AbstractModule
  {

    @Override
    protected void configure()
    {
      bind(QuerySegmentWalker.class).toInstance(mockServerManager);
      bind(SegmentLoader.class).toInstance(mockSegmentLoader);
      bind(SegmentCacheManager.class).toInstance(mockCacheManager);
    }
  }

}
