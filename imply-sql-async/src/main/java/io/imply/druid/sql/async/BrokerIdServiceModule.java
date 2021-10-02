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
import io.imply.druid.sql.async.discovery.BrokerIdService;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class BrokerIdServiceModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(SqlAsyncModule.class.getSimpleName())
            .registerSubtypes(new NamedType(BrokerIdService.class, BrokerIdService.NAME))
    );
  }
}
