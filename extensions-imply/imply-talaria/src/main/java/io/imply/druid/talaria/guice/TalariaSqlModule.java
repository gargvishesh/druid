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
import io.imply.druid.license.ImplyLicenseManager;
import io.imply.druid.talaria.sql.ImplyQueryMakerFactory;
import io.imply.druid.talaria.sql.NoopQueryMakerFactory;
import io.imply.druid.talaria.sql.TalariaExternalOperatorConversion;
import org.apache.druid.client.indexing.HttpIndexingServiceClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TalariaSqlModule implements DruidModule
{
  @Inject
  ImplyLicenseManager implyLicenseManager = null;

  @Inject
  Properties properties = null;

  @Override
  public List<? extends Module> getJacksonModules()
  {
    if (!implyLicenseManager.isFeatureEnabled(TalariaModules.FEATURE_NAME)) {
      return Collections.emptyList();
    }

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
    if (!implyLicenseManager.isFeatureEnabled(TalariaModules.FEATURE_NAME)) {
      return;
    }

    // We want this module to bring InputSourceModule along for the ride.
    binder.install(new InputSourceModule());

    // Set up the EXTERN macro.
    SqlBindings.addOperatorConversion(binder, TalariaExternalOperatorConversion.class);

    // Set up the ImplyQueryMakerFactory.
    PolyBind.optionBinder(binder, Key.get(QueryMakerFactory.class))
            .addBinding(ImplyQueryMakerFactory.TYPE)
            .to(Key.get(QueryMakerFactory.class, Talaria.class))
            .in(LazySingleton.class);

    binder.bind(IndexingServiceClient.class).to(HttpIndexingServiceClient.class).in(LazySingleton.class);
  }

  @Provides
  @Talaria
  public QueryMakerFactory buildTalariaQueryMakerFactory(final Injector injector)
  {
    if (!implyLicenseManager.isFeatureEnabled(TalariaModules.FEATURE_NAME)) {
      // This provider will not be used if none of the other Talaria stuff is bound, so this exception will
      // not actually be reached in production. But include it here anyway, to make it clear that it is only
      // expected to be used in concert with the rest of the extension.
      throw new ISE("Not used");
    } else if (TalariaModules.getNodeRoles(injector).contains(NodeRole.BROKER)) {
      return injector.getInstance(ImplyQueryMakerFactory.class);
    } else {
      return new NoopQueryMakerFactory();
    }
  }
}
