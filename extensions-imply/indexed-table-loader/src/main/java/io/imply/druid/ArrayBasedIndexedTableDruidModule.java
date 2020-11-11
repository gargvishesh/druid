/*
 * Copyright (c) 2019 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import io.imply.druid.segment.OnHeapIndexedTableSegmentizerFactory;
import io.imply.druid.segment.join.IndexedTableJoinableFactory;
import io.imply.druid.segment.join.IndexedTableManager;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.segment.join.JoinableFactory;

import java.util.List;

public class ArrayBasedIndexedTableDruidModule implements DruidModule
{
  private static final String INDEXED_TABLE_SEGMENTIZER_TYPE = "onHeapIndexedTable";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("ArrayBasedIndexedTableModule").registerSubtypes(
            new NamedType(OnHeapIndexedTableSegmentizerFactory.class, INDEXED_TABLE_SEGMENTIZER_TYPE)
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    binder.bind(IndexedTableManager.class).in(LazySingleton.class);
    MapBinder<Class<? extends JoinableFactory>, Class<? extends DataSource>> joinableFactoryMappingBinder =
        DruidBinders.joinableMappingBinder(binder);

    Multibinder<JoinableFactory> joinableFactoryMultibinder = DruidBinders.joinableFactoryMultiBinder(binder);

    joinableFactoryMultibinder.addBinding().to(IndexedTableJoinableFactory.class);
    joinableFactoryMappingBinder.addBinding(IndexedTableJoinableFactory.class).toInstance(GlobalTableDataSource.class);
  }
}
