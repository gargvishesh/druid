/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.lookup;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.imply.druid.util.CronFactory;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.server.SegmentManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class exists to be the place that contains forked changes to druid's {@link org.apache.druid.query.lookup.LookupModule}
 * <p>
 * It exists so that we can hopefully avoid merge conflicts in imports as well as other things by allowing for us to
 * replace singular lines with fully-qualified static calls into this code.
 */
public class ImplyLookupModule implements DruidModule
{
  private static final HashSet<NodeRole> ROLES_TO_DO_GUICE_BINDING = new HashSet<>(
      Arrays.asList(NodeRole.BROKER, NodeRole.HISTORICAL, NodeRole.PEON, NodeRole.INDEXER)
  );

  private Set<NodeRole> roles;

  @Inject
  public void injectMe(
      @Self Set<NodeRole> roles
  )
  {
    this.roles = roles;
  }

  @Override
  public void configure(Binder binder)
  {
    boolean doGuiceBinding = false;
    for (NodeRole nodeRole : roles) {
      if (ROLES_TO_DO_GUICE_BINDING.contains(nodeRole)) {
        doGuiceBinding = true;
        break;
      }
    }

    if (doGuiceBinding) {
      // This binding does not carry a singleton because it is just binding the interface, the provider for the
      // concrete class ImplyLookupProvider carries the singleton annotation instead.
      binder.bind(LookupExtractorFactoryContainerProvider.class).to(ImplyLookupProvider.class);
      binder.bind(ImplyLookupProvider.class).in(LazySingleton.class);


      binder.bind(LookupExtractorFactoryContainerProvider.class)
            .annotatedWith(ImplyLookup.class)
            .to(LookupReferencesManager.class);
      binder.bind(SegmentManager.class).to(SegmentManagerWithCallbacks.class).in(LazySingleton.class);

      ScheduledExecutorService executor = ScheduledExecutors.fixed(1, "SegmentFiltered");
      binder.bind(CronFactory.class).annotatedWith(ImplyLookup.class).toInstance(new CronFactory(executor));
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("ImplyLookupModule").registerSubtypes(
            new NamedType(SegmentFilteredLookupExtractorFactory.class, "implySegment")
        )
    );
  }
}
