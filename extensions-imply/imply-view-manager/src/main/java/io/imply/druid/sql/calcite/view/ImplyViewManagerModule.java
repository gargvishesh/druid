/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Key;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.sql.calcite.view.ViewManager;

import java.util.Collections;
import java.util.List;

public class ImplyViewManagerModule implements DruidModule
{
  public static final String TYPE = "imply";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    PolyBind.optionBinder(binder, Key.get(ViewManager.class))
            .addBinding(TYPE)
            .to(ImplyViewManager.class)
            .in(LazySingleton.class);

    LifecycleModule.register(binder, TemporaryViewDefinitionLoader.class);
  }
}
