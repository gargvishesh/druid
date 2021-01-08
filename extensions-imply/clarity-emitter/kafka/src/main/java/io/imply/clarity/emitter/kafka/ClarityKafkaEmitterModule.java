/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.kafka;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.imply.clarity.emitter.BaseClarityEmitterConfig;
import io.imply.clarity.emitter.ClarityNodeDetails;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.emitter.core.Emitter;

import java.util.Collections;
import java.util.List;

public class ClarityKafkaEmitterModule implements DruidModule
{
  private static final String EMITTER_TYPE = "clarity-kafka";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.clarity", ClarityKafkaEmitterConfig.class);
    binder.bind(BaseClarityEmitterConfig.class).to(ClarityKafkaEmitterConfig.class);
  }

  @Provides
  @ManageLifecycle
  @Named(EMITTER_TYPE)
  public Emitter getEmitter(
      Supplier<ClarityKafkaEmitterConfig> configSupplier,
      @Json ObjectMapper jsonMapper,
      Injector injector
  )
  {
    final ClarityKafkaEmitterConfig config = configSupplier.get();
    return new ClarityKafkaEmitter(ClarityNodeDetails.fromInjector(injector, config), config, jsonMapper);
  }
}
