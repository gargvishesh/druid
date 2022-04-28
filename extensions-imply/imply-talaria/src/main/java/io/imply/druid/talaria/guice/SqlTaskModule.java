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
import com.google.inject.Binder;
import io.imply.druid.talaria.api.SqlTaskResource;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class SqlTaskModule implements DruidModule
{

  @Override
  public void configure(Binder binder)
  {
    // Force eager initialization.
    LifecycleModule.register(binder, SqlTaskResource.class);
    Jerseys.addResource(binder, SqlTaskResource.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }
}
