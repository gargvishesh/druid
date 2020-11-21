/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.curator.discovery.CuratorDruidLeaderSelector;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.ZkPathsConfig;

import java.util.function.Function;

public class IngestServiceDiscoveryModule implements Module
{
  // these are copied from DiscoveryModule
  private static final String INTERNAL_DISCOVERY_PROP = "druid.discovery.type";
  private static final String CURATOR_KEY = "curator";

  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoiceWithDefault(
        binder,
        INTERNAL_DISCOVERY_PROP,
        Key.get(DruidLeaderSelector.class, IngestionService.class),
        CURATOR_KEY
    );
    PolyBind.optionBinder(binder, Key.get(DruidLeaderSelector.class, IngestionService.class))
            .addBinding(CURATOR_KEY)
            .toProvider(
                new IngestServiceLeaderSelectorProvider(
                    zkPathsConfig -> ZKPaths.makePath(zkPathsConfig.getBase(), "ingest", "_INGEST")
                )
            )
            .in(LazySingleton.class);
  }

  private static class IngestServiceLeaderSelectorProvider implements Provider<DruidLeaderSelector>
  {
    @Inject
    private Provider<CuratorFramework> curatorFramework;

    @Inject
    @Self
    private DruidNode druidNode;

    @Inject
    private ZkPathsConfig zkPathsConfig;

    private final Function<ZkPathsConfig, String> latchPathFn;

    IngestServiceLeaderSelectorProvider(Function<ZkPathsConfig, String> latchPathFn)
    {
      this.latchPathFn = latchPathFn;
    }

    @Override
    public DruidLeaderSelector get()
    {
      return new CuratorDruidLeaderSelector(
          curatorFramework.get(),
          druidNode,
          latchPathFn.apply(zkPathsConfig)
      );
    }
  }
}
