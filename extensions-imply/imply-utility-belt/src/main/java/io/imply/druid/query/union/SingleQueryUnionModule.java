/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.union;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.QueryToolChestWarehouse;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class SingleQueryUnionModule implements DruidModule
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
      DruidBinders.queryToolChestBinder(binder)
                  .addBinding(SingleQueryUnionQuery.class)
                  .to(SingleQueryUnionQueryToolChest.class)
                  .in(LazySingleton.class);

      binder.bind(
                new TypeLiteral<Supplier<QueryToolChestWarehouse>>()
                {
                }
            )
            .toProvider(new ProviderBasedJavaSupplierProvider<>(Key.get(QueryToolChestWarehouse.class)))
            .in(LazySingleton.class);
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("SingleQueryUnionModule").registerSubtypes(
            new NamedType(SingleQueryUnionQuery.class, "singleQueryUnion")
        )
    );
  }
}