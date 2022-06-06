/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.imply.druid.talaria.sql.ImplyQueryMakerFactory;
import io.imply.druid.talaria.sql.NoopQueryMakerFactory;
import io.imply.druid.talaria.sql.TalariaExternalOperatorConversion;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TalariaSqlModule implements DruidModule
{
  @Inject
  Properties properties = null;

  @Override
  public List<? extends Module> getJacksonModules()
  {
    final List<Module> modules = new ArrayList<>();

    modules.add(
        new SimpleModule(getClass().getSimpleName()).registerSubtypes(
            ExternalDataSource.class
        )
    );

    // We want this module to bring InputSourceModule along for the ride.
    modules.addAll(new InputSourceModule().getJacksonModules());
    return modules;
  }

  @Override
  public void configure(Binder binder)
  {
    // We want this module to bring InputSourceModule along for the ride.
    binder.install(new InputSourceModule());

    // Set up the EXTERN macro.
    SqlBindings.addOperatorConversion(binder, TalariaExternalOperatorConversion.class);

    // Set up the ImplyQueryMakerFactory.
    PolyBind.optionBinder(binder, Key.get(QueryMakerFactory.class))
            .addBinding(ImplyQueryMakerFactory.TYPE)
            .to(Key.get(QueryMakerFactory.class, Talaria.class))
            .in(LazySingleton.class);
  }

  @Provides
  @Talaria
  public QueryMakerFactory buildTalariaQueryMakerFactory(final Injector injector)
  {
    if (TalariaModules.getNodeRoles(injector).contains(NodeRole.BROKER)) {
      return injector.getInstance(ImplyQueryMakerFactory.class);
    } else {
      return new NoopQueryMakerFactory();
    }
  }
}
