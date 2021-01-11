/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.samples.caffeine;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.imply.druid.ingest.samples.SampleStore;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Collections;
import java.util.List;

public class CaffeineSampleStoreModule implements DruidModule
{
  public static final String TYPE = "caffeine";

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
        StringUtils.format("%s.%s", SampleStore.STORE_PROPERTY_BASE, TYPE),
        CaffeineSampleStoreConfig.class
    );

    PolyBind.optionBinder(binder, Key.get(SampleStore.class))
            .addBinding(TYPE)
            .to(CaffeineSampleStore.class)
            .in(LazySingleton.class);
  }
}
