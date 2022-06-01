/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.multibindings.MultibindingsScanner;
import com.google.inject.multibindings.ProvidesIntoSet;
import com.google.inject.name.Named;
import io.imply.druid.sql.async.discovery.BrokerIdService;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

@LoadScope(roles = {NodeRole.BROKER_JSON_NAME, NodeRole.ROUTER_JSON_NAME, NodeRole.COORDINATOR_JSON_NAME})
public class BrokerIdServiceModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.install(MultibindingsScanner.asModule());
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(BrokerIdServiceModule.class.getSimpleName())
            .registerSubtypes(new NamedType(BrokerIdService.class, BrokerIdService.NAME))
    );
  }

  @ProvidesIntoSet
  @Named(NodeRole.BROKER_JSON_NAME)
  public Class<? extends DruidService> getBrokerIdService()
  {
    return BrokerIdService.class;
  }
}
